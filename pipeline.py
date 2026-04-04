# pipeline.py — orchestrates scrape → store → analyze → save

import logging
import os
import random

import boto3
import requests

from agent import get_movie_metadata, select_screencaps
from config import AWS_REGION, BASE_URL, DEFAULT_PAGES_TO_SCRAPE, S3_BUCKET, TARGET_SCREENCAP_COUNT, USER_AGENT
from scraper import get_image_urls, get_total_pages, sample_pages, search
from storage import (
    ensure_bucket,
    ensure_table,
    save_movie,
    update_screencaps,
)

_s3 = boto3.client("s3", region_name=AWS_REGION)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run(query: str, pages: int = DEFAULT_PAGES_TO_SCRAPE) -> dict:
    """
    Full pipeline:
      1. Search for movie
      2. Scrape X random pages of screencaps → upload to S3
      3. Get movie metadata from Claude
      4. Analyze all uploaded images → select 10
      5. Save everything to DynamoDB
    """
    # --- Setup AWS resources ---
    ensure_bucket()
    ensure_table()

    # --- Phase 1: Search ---
    logger.info(f"Searching for: {query!r}")
    results = search(query)
    if not results:
        raise ValueError(f"No movies found for query: {query!r}")

    # Take the first result
    movie = results[0]
    logger.info(f"Selected: {movie.title} ({movie.url})")

    # --- Phase 2: Collect image URLs (no downloading yet) ---
    total_pages = get_total_pages(movie.url)
    logger.info(f"Total pages available: {total_pages}")

    sampled = sample_pages(total_pages, n=pages)
    logger.info(f"Sampling pages: {sampled}")

    all_image_urls = []  # (page_num, idx, url)
    for page_num in sampled:
        image_urls = get_image_urls(movie.url, page_num)
        for idx, img_url in enumerate(image_urls):
            all_image_urls.append((page_num, idx, img_url))

    logger.info(f"Collected {len(all_image_urls)} image URLs across {len(sampled)} pages")

    # Shuffle aggressively — multiple passes to break any ordering patterns
    for _ in range(3):
        random.shuffle(all_image_urls)
    logger.info("Shuffled image list for diverse batch evaluation")

    # --- Phase 3: Movie metadata via Claude ---
    logger.info("Fetching movie metadata from Claude...")
    metadata = get_movie_metadata(movie.title)
    logger.info(f"Metadata: {metadata}")
    metadata["s3_prefix"] = f"movies/{movie.movie_id}/"
    metadata["status"] = "pending"
    metadata["movie_screen_caps"] = []
    save_movie(movie.movie_id, metadata)

    # --- Phase 4: Agent evaluates raw URLs, picks 10 ---
    logger.info(f"Sending {len(all_image_urls)} image URLs to Claude for evaluation...")
    raw_urls = [url for _, _, url in all_image_urls]
    selection = select_screencaps(movie.title, raw_urls, metadata=metadata)
    selection.print_summary()
    approved_urls = selection.approved_urls

    # --- Save approved images locally (use /tmp in Lambda) + upload to S3 ---
    output_dir = f"/tmp/output/{movie.movie_id}"
    os.makedirs(output_dir, exist_ok=True)
    approved_keys = []
    for i, url in enumerate(approved_urls, 1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT, "Referer": BASE_URL}, timeout=15)
            resp.raise_for_status()
            ext = url.split(".")[-1].split("?")[0] or "jpg"
            # Save locally
            filepath = f"{output_dir}/{i:02d}.{ext}"
            with open(filepath, "wb") as f:
                f.write(resp.content)
            logger.info(f"Saved locally: {filepath}")
            # Upload to S3
            key = f"movies/{movie.movie_id}/{i:02d}.{ext}"
            _s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=resp.content,
                ContentType=f"image/{ext}",
            )
            logger.info(f"Uploaded to S3: {key}")
            approved_keys.append(key)
        except Exception as e:
            logger.warning(f"Failed to process {url}: {e}")

    # --- Save final results to DynamoDB ---
    update_screencaps(movie.movie_id, approved_keys)

    logger.info(f"\n=== Done. {len(approved_keys)} screencaps saved for {movie.title!r} ===")
    for i, key in enumerate(approved_keys, 1):
        logger.info(f"  [{i}] s3://{S3_BUCKET}/{key}")

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
    result = run(query)
    print(result)
