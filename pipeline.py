# pipeline.py — orchestrates scrape → store → analyze → save

import logging
import os
import random

import boto3
import requests

from agent import get_movie_metadata, get_similar_movies, select_screencaps
from config import AWS_REGION, BASE_URL, DEFAULT_PAGES_TO_SCRAPE, S3_BUCKET, SPREAD_INDEXES_PER_PAGE, IMAGES_TO_SHOW_CAP, USER_AGENT
from scraper import get_image_urls, get_total_pages, sample_pages, spread_sample, search, MovieEntry
from storage import (
    ensure_bucket,
    ensure_table,
    save_movie,
    update_screencaps,
)

_s3 = boto3.client("s3", region_name=AWS_REGION)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _refresh_movie(movie_id: str, caps: list, expires: int = 86400):
    """Populate images_to_show with 80/20 ratio using has_people metadata."""
    import random as _rand
    from config import DYNAMO_TABLE, IMAGES_TO_SHOW_CAP
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    # Split by has_people
    no_people = [c for c in caps if not (c.get("has_people") if isinstance(c, dict) else False)]
    with_people = [c for c in caps if (c.get("has_people") if isinstance(c, dict) else False)]

    # 80/20 ratio
    n_no_people = min(int(IMAGES_TO_SHOW_CAP * 0.8), len(no_people))
    n_with_people = min(IMAGES_TO_SHOW_CAP - n_no_people, len(with_people))
    # Fill remaining slots if one pool is short
    if n_no_people + n_with_people < IMAGES_TO_SHOW_CAP:
        extra = IMAGES_TO_SHOW_CAP - n_no_people - n_with_people
        n_no_people = min(n_no_people + extra, len(no_people))

    selected = _rand.sample(no_people, n_no_people) + _rand.sample(with_people, n_with_people)
    _rand.shuffle(selected)

    selected_keys = [c["key"] if isinstance(c, dict) else c for c in selected]
    presigned = []
    for key in selected_keys:
        try:
            url = _s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires)
            presigned.append(url)
        except Exception as e:
            logger.warning(f"Presigned URL failed for {key}: {e}")

    # Hard mode: no people, no iconic scenes
    hard_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people") and not c.get("iconic_scene")]
    if not hard_pool:
        hard_pool = no_people
    hard_selected = _rand.sample(hard_pool, min(IMAGES_TO_SHOW_CAP, len(hard_pool)))
    _rand.shuffle(hard_selected)
    hard_keys = [c["key"] if isinstance(c, dict) else c for c in hard_selected]
    hard_presigned = []
    for key in hard_keys:
        try:
            url = _s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires)
            hard_presigned.append(url)
        except Exception as e:
            logger.warning(f"Hard presigned URL failed for {key}: {e}")
    table.update_item(
        Key={"movie_id": movie_id},
        UpdateExpression="SET images_to_show = :k, presigned_urls = :u, hard_images_to_show = :hk, hard_presigned_urls = :hu",
        ExpressionAttributeValues={":k": selected_keys, ":u": presigned, ":hk": hard_keys, ":hu": hard_presigned},
    )
    logger.info(f"images_to_show set for {movie_id} ({n_no_people} env + {n_with_people} people, {len(hard_keys)} hard)")


def run(query: str, pages: int = DEFAULT_PAGES_TO_SCRAPE, url: str = None) -> dict:
    """
    Full pipeline:
      1. Search for movie (or use direct URL)
      2. Scrape X random pages of screencaps → upload to S3
      3. Get movie metadata from Claude
      4. Analyze all uploaded images → select 10
      5. Save everything to DynamoDB
    """
    # --- Setup AWS resources ---
    ensure_bucket()
    ensure_table()

    # --- Phase 1: Search or use direct URL ---
    if url:
        slug = url.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ").title()
        movie = MovieEntry(title=title, url=url, movie_id=slug)
        logger.info(f"Using direct URL: {url}")
    else:
        import re
        search_query = re.sub(r'\s+4[Kk]\b', '', query).strip()
        logger.info(f"Searching for: {search_query!r}")
        results = search(search_query)
        if not results and search_query != query:
            logger.info(f"Retrying with original query: {query!r}")
            results = search(query)
        if not results:
            raise ValueError(f"No movies found for query: {query!r}")
        movie = results[0]
        logger.info(f"Selected: {movie.title} ({movie.url})")

    # --- Skip if already processed ---
    existing = _s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"movies/{movie.movie_id}/", MaxKeys=1)
    if existing.get("KeyCount", 0) > 0 and not os.environ.get("FORCE_REPROCESS"):
        logger.info(f"Skipping {movie.title!r} — already has screencaps in S3")
        return {"movie_id": movie.movie_id, "title": movie.title, "skipped": True, "movie_screen_caps": []}

    # --- Phase 2: Collect image URLs (no downloading yet) ---
    total_pages = get_total_pages(movie.url)
    logger.info(f"Total pages available: {total_pages}")

    all_image_urls = []
    for page_num in range(1, total_pages + 1):
        image_urls = get_image_urls(movie.url, page_num)
        spread = spread_sample(image_urls, SPREAD_INDEXES_PER_PAGE)
        for idx, img_url in enumerate(spread):
            all_image_urls.append((page_num, idx, img_url))

    logger.info(f"Collected {len(all_image_urls)} image URLs ({SPREAD_INDEXES_PER_PAGE} spread per page × {total_pages} pages)")

    # Shuffle aggressively — multiple passes to break any ordering patterns
    for _ in range(3):
        random.shuffle(all_image_urls)
    logger.info("Shuffled image list for diverse batch evaluation")

    # --- Phase 3: Movie metadata via Claude ---
    logger.info("Fetching movie metadata from Claude...")
    metadata = get_movie_metadata(movie.title)
    logger.info(f"Metadata: {metadata}")

    # Generate similar movies
    similar = get_similar_movies(metadata)
    if similar:
        metadata["similar_movies"] = similar
        logger.info(f"Similar movies: {similar}")

    metadata["s3_prefix"] = f"movies/{movie.movie_id}/"
    metadata["status"] = "pending"
    metadata["movie_screen_caps"] = []
    metadata["source_url"] = movie.url
    save_movie(movie.movie_id, metadata)

    # --- Phase 4: Agent evaluates raw URLs, picks 10 ---
    logger.info(f"Sending {len(all_image_urls)} image URLs to Claude for evaluation...")
    raw_urls = [url for _, _, url in all_image_urls]
    selection = select_screencaps(movie.title, raw_urls, metadata=metadata)
    selection.print_summary()
    approved_urls = selection.approved_urls

    local_test = bool(os.environ.get("LOCAL_TEST"))

    # --- Save approved images ---
    output_dir = f"output/{movie.movie_id}" if local_test else f"/tmp/output/{movie.movie_id}"
    import shutil
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    approved_keys = []
    image_meta = []  # metadata per image for local persistence
    for i, item in enumerate(approved_urls, 1):
        url = item["url"] if isinstance(item, dict) else item
        has_people = item.get("has_people", False) if isinstance(item, dict) else False
        has_cast = item.get("has_cast", False) if isinstance(item, dict) else False
        iconic_scene = item.get("iconic_scene", False) if isinstance(item, dict) else False
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT, "Referer": BASE_URL}, timeout=15)
            resp.raise_for_status()
            ext = url.split(".")[-1].split("?")[0] or "jpg"
            filepath = f"{output_dir}/{i:02d}.{ext}"
            with open(filepath, "wb") as f:
                f.write(resp.content)
            logger.info(f"Saved: {filepath} {'🎭' if has_cast else '👤' if has_people else '🏠'}{'⭐' if iconic_scene else ''}")

            entry = {"key": f"movies/{movie.movie_id}/{i:02d}.{ext}", "has_people": has_people, "has_cast": has_cast, "iconic_scene": iconic_scene}
            image_meta.append(entry)

            if not local_test:
                key = entry["key"]
                _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=resp.content, ContentType=f"image/{ext}")
                logger.info(f"Uploaded to S3: {key}")
                approved_keys.append(entry)
        except Exception as e:
            logger.warning(f"Failed to process {url}: {e}")

    # Always save metadata JSON locally
    import json as _json
    meta_path = f"{output_dir}/metadata.json"
    with open(meta_path, "w") as f:
        _json.dump(image_meta, f, indent=2)
    logger.info(f"Saved metadata: {meta_path} ({len(image_meta)} images)")

    if not local_test:
        update_screencaps(movie.movie_id, approved_keys)
        _refresh_movie(movie.movie_id, approved_keys)

    logger.info(f"\n=== Done. {len(approved_urls)} screencaps saved for {movie.title!r} ===")
    logger.info(f"  Output: {output_dir}")

    return {
        "movie_id": movie.movie_id,
        "title": movie.title,
        "movie_screen_caps": approved_keys,
        "output_dir": output_dir,
        "total_candidates": len(all_image_urls),
    }


if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Inception"
    # If arg looks like a URL, pass it as url param
    if query.startswith("http"):
        result = run("", url=query)
    else:
        result = run(query)
    print(result)
