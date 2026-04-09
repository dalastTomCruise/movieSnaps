"""
api_handler.py — API Gateway Lambda handler for the movie screencaps game.

Routes:
  GET /random-movie  — returns a random approved movie with presigned image URLs
  GET /movie/{id}    — returns a specific movie with presigned image URLs
"""

import json
import logging
import random
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from config import AWS_REGION, DYNAMO_TABLE, LEADERBOARD_TABLE, S3_BUCKET

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_s3 = boto3.client("s3", region_name=AWS_REGION)
_dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {**CORS_HEADERS, "Content-Type": "application/json"},
        "body": json.dumps(body, default=lambda o: int(o) if isinstance(o, Decimal) else str(o)),
    }


def enrich_with_urls(movie: dict, expires: int = 3600) -> dict:
    """
    Serve presigned_urls directly from DB if available (generated nightly, valid 24h).
    Falls back to generating on-the-fly from movie_screen_caps if not present.
    Also serves hard_images_to_show for hard mode.
    """
    # Normal mode
    if movie.get("presigned_urls"):
        movie["images_to_show"] = movie["presigned_urls"]
    else:
        show_keys = movie.get("images_to_show", [])
        show_urls = []
        for key in show_keys:
            try:
                url = _s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires)
                show_urls.append(url)
            except Exception as e:
                logger.warning(f"Failed to generate presigned URL for {key}: {e}")
        movie["images_to_show"] = show_urls

    # Hard mode
    if movie.get("hard_presigned_urls"):
        movie["hard_images_to_show"] = movie["hard_presigned_urls"]
    else:
        hard_keys = movie.get("hard_images_to_show", [])
        hard_urls = []
        for key in hard_keys:
            try:
                url = _s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires)
                hard_urls.append(url)
            except Exception as e:
                logger.warning(f"Failed to generate hard presigned URL for {key}: {e}")
        movie["hard_images_to_show"] = hard_urls

    # Full screencap list with metadata (for dev UI)
    keys = movie.get("movie_screen_caps", [])
    all_images = []
    for item in keys:
        if isinstance(item, dict):
            k = item["key"]
            meta = {"has_people": item.get("has_people", False), "has_cast": item.get("has_cast", False), "iconic_scene": item.get("iconic_scene", False)}
        else:
            k = item
            meta = {"has_people": False, "has_cast": False, "iconic_scene": False}
        try:
            url = _s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=expires)
            all_images.append({"url": url, **meta})
        except Exception as e:
            logger.warning(f"Failed to generate presigned URL for {k}: {e}")
    movie["image_urls"] = [img["url"] for img in all_images]  # backwards compat
    movie["all_images"] = all_images  # dev UI — includes metadata per image

    return movie


def has_caps(movie: dict) -> bool:
    return bool(movie.get("movie_screen_caps"))


def get_random_movie(exclude: list[str] = None) -> dict | None:
    table = _dynamo.Table(DYNAMO_TABLE)
    result = table.scan(FilterExpression=Attr("status").eq("approved"))
    items = [m for m in result.get("Items", []) if has_caps(m)]
    if exclude:
        items = [m for m in items if m["movie_id"] not in exclude]
    if not items:
        return None
    return random.choice(items)


import re

def clean_title(title: str, year) -> str:
    """Strip year suffix appended by pipeline e.g. 'Pulp Fiction 1994' -> 'Pulp Fiction'"""
    if not title:
        return title
    # Remove trailing year like ' 1994' or ' (1994)'
    cleaned = re.sub(r'\s*\(?\d{4}\)?$', '', title.strip())
    return cleaned.strip() or title


def get_all_movies() -> list[dict]:
    """Returns lightweight list of all approved movies for search/hard mode."""
    table = _dynamo.Table(DYNAMO_TABLE)
    result = table.scan(
        FilterExpression=Attr("status").eq("approved"),
        ProjectionExpression="movie_id, title, #yr, scraped_at, updated_at",
        ExpressionAttributeNames={"#yr": "year"},
    )
    movies = []
    for m in result.get("Items", []):
        title = m.get("title") or m.get("movie_id", "")
        year = m.get("year")
        movies.append({
            "movie_id": m["movie_id"],
            "title": clean_title(title, year),
            "year": int(year) if year else None,
            "scraped_at": m.get("scraped_at"),
            "updated_at": m.get("updated_at"),
        })
    return sorted(movies, key=lambda x: x["title"] or "")


def get_movies_by_decade(decade: int, exclude: list[str] = None) -> list[dict]:
    """Returns all approved movies from a given decade, shuffled."""
    table = _dynamo.Table(DYNAMO_TABLE)
    decade_start = decade
    decade_end = decade + 9
    result = table.scan(FilterExpression=Attr("status").eq("approved"))
    items = [
        m for m in result.get("Items", [])
        if has_caps(m) and m.get("year") is not None and decade_start <= int(m.get("year", 0)) <= decade_end
    ]
    if exclude:
        items = [m for m in items if m["movie_id"] not in exclude]
    random.shuffle(items)
    return items


def get_movie_by_id(movie_id: str) -> dict | None:
    table = _dynamo.Table(DYNAMO_TABLE)
    resp = table.get_item(Key={"movie_id": movie_id})
    item = resp.get("Item")
    if item and item.get("status") == "approved" and has_caps(item):
        return item
    logger.warning(f"Movie '{movie_id}' has no caps, falling back to random")
    return get_random_movie()


def get_available_decades() -> list[dict]:
    """Returns all decades that have at least one approved movie with caps."""
    table = _dynamo.Table(DYNAMO_TABLE)
    result = table.scan(FilterExpression=Attr("status").eq("approved"))
    decade_counts = {}
    for m in result.get("Items", []):
        if not has_caps(m):
            continue
        year = m.get("year")
        if year is None:
            continue
        try:
            year = int(year)
        except (ValueError, TypeError):
            continue
        if year:
            decade = (year // 10) * 10
            decade_counts[decade] = decade_counts.get(decade, 0) + 1
    return sorted([{"decade": d, "count": c} for d, c in decade_counts.items()], key=lambda x: x["decade"])
    """Returns all approved movies from a given decade, shuffled."""
    table = _dynamo.Table(DYNAMO_TABLE)
    decade_start = decade
    decade_end = decade + 9
    result = table.scan(FilterExpression=Attr("status").eq("approved"))
    items = [
        m for m in result.get("Items", [])
        if has_caps(m) and decade_start <= int(m.get("year", 0)) <= decade_end
    ]
    if exclude:
        items = [m for m in items if m["movie_id"] not in exclude]
    random.shuffle(items)
    return items
    table = _dynamo.Table(DYNAMO_TABLE)
    resp = table.get_item(Key={"movie_id": movie_id})
    item = resp.get("Item")
    if item and item.get("status") == "approved" and has_caps(item):
        return item
    # Requested movie has no caps — fall back to a random one that does
    logger.warning(f"Movie '{movie_id}' has no caps, falling back to random")
    return get_random_movie()


def handler(event, context):
    method = event.get("httpMethod", "GET")
    path = event.get("path", "/")
    path_params = event.get("pathParameters") or {}

    # Handle CORS preflight
    if method == "OPTIONS":
        return response(200, {})

    # --- Refresh images_to_show for a movie (dev UI) ---
    if path.startswith("/movie/") and path.endswith("/refresh") and method == "POST":
        try:
            movie_id = path.split("/movie/")[1].split("/refresh")[0]
            table = _dynamo.Table(DYNAMO_TABLE)
            item = table.get_item(Key={"movie_id": movie_id}).get("Item", {})
            caps = item.get("movie_screen_caps", [])
            if not caps:
                return response(404, {"error": "No screencaps found"})

            from config import IMAGES_TO_SHOW_CAP
            from datetime import datetime, timezone

            cast_pool = [c for c in caps if isinstance(c, dict) and c.get("has_cast")]
            extras_pool = [c for c in caps if isinstance(c, dict) and c.get("has_people") and not c.get("has_cast")]
            empty_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people")]
            if not cast_pool and not extras_pool and not empty_pool:
                empty_pool = [{"key": c} if isinstance(c, str) else c for c in caps]

            selected = []
            if cast_pool:
                selected += random.sample(cast_pool, min(1, len(cast_pool)))
            if extras_pool:
                selected += random.sample(extras_pool, min(1, len(extras_pool)))
            remaining = IMAGES_TO_SHOW_CAP - len(selected)
            if empty_pool:
                selected += random.sample(empty_pool, min(remaining, len(empty_pool)))
            random.shuffle(selected)

            selected_keys = [c["key"] if isinstance(c, dict) else c for c in selected]
            presigned = [_s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in selected_keys]

            hard_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people") and not c.get("iconic_scene")]
            if not hard_pool:
                hard_pool = empty_pool
            hard_selected = random.sample(hard_pool, min(IMAGES_TO_SHOW_CAP, len(hard_pool)))
            hard_keys = [c["key"] if isinstance(c, dict) else c for c in hard_selected]
            hard_presigned = [_s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in hard_keys]

            now = datetime.now(timezone.utc).isoformat()
            table.update_item(
                Key={"movie_id": movie_id},
                UpdateExpression="SET images_to_show = :k, presigned_urls = :u, hard_images_to_show = :hk, hard_presigned_urls = :hu, updated_at = :ts",
                ExpressionAttributeValues={":k": selected_keys, ":u": presigned, ":hk": hard_keys, ":hu": hard_presigned, ":ts": now},
            )
            return response(200, {"movie_id": movie_id, "images_to_show": len(selected_keys), "hard_images_to_show": len(hard_keys), "updated_at": now})
        except Exception as e:
            logger.error(f"Refresh failed: {e}")
            return response(500, {"error": str(e)})

    # --- Delete an image (dev UI) ---
    if path.startswith("/movie/") and path.endswith("/delete-image") and method == "POST":
        try:
            movie_id = path.split("/movie/")[1].split("/delete-image")[0]
            body = json.loads(event.get("body", "{}"))
            image_key = body.get("key")
            if not image_key:
                return response(400, {"error": "key required"})

            table = _dynamo.Table(DYNAMO_TABLE)
            item = table.get_item(Key={"movie_id": movie_id}).get("Item", {})
            caps = item.get("movie_screen_caps", [])

            new_caps = [c for c in caps if not ((isinstance(c, dict) and c.get("key") == image_key) or (isinstance(c, str) and c == image_key))]
            if len(new_caps) == len(caps):
                return response(404, {"error": f"Image key not found: {image_key}"})

            # Delete from S3
            try:
                _s3.delete_object(Bucket=S3_BUCKET, Key=image_key)
            except Exception as e:
                logger.warning(f"S3 delete failed for {image_key}: {e}")

            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            table.update_item(
                Key={"movie_id": movie_id},
                UpdateExpression="SET movie_screen_caps = :caps, updated_at = :ts",
                ExpressionAttributeValues={":caps": new_caps, ":ts": now},
            )
            return response(200, {"movie_id": movie_id, "deleted": image_key, "remaining": len(new_caps), "updated_at": now})
        except Exception as e:
            logger.error(f"Delete image failed: {e}")
            return response(500, {"error": str(e)})

    # --- Update image metadata (dev UI) ---
    if path.startswith("/movie/") and path.endswith("/image-meta") and method == "POST":
        try:
            movie_id = path.split("/movie/")[1].split("/image-meta")[0]
            body = json.loads(event.get("body", "{}"))
            image_key = body.get("key")
            if not image_key:
                return response(400, {"error": "key required"})

            table = _dynamo.Table(DYNAMO_TABLE)
            item = table.get_item(Key={"movie_id": movie_id}).get("Item", {})
            caps = item.get("movie_screen_caps", [])

            updated = False
            for cap in caps:
                if isinstance(cap, dict) and cap.get("key") == image_key:
                    if "has_people" in body:
                        cap["has_people"] = bool(body["has_people"])
                    if "has_cast" in body:
                        cap["has_cast"] = bool(body["has_cast"])
                    if "iconic_scene" in body:
                        cap["iconic_scene"] = bool(body["iconic_scene"])
                    updated = True
                    break

            if not updated:
                return response(404, {"error": f"Image key not found: {image_key}"})

            from datetime import datetime, timezone
            table.update_item(
                Key={"movie_id": movie_id},
                UpdateExpression="SET movie_screen_caps = :caps, updated_at = :ts",
                ExpressionAttributeValues={":caps": caps, ":ts": datetime.now(timezone.utc).isoformat()},
            )
            return response(200, {"movie_id": movie_id, "key": image_key, "updated": True})
        except Exception as e:
            logger.error(f"Image meta update failed: {e}")
            return response(500, {"error": str(e)})

    # --- Leaderboard ---
    if path == "/score" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            username = body.get("username", "").strip()[:20]
            score = int(body.get("score", 0))
            if not username:
                return response(400, {"error": "username required"})
            from datetime import datetime, timezone
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            lb = _dynamo.Table(LEADERBOARD_TABLE)
            lb.put_item(Item={"username": username, "score": score, "date": today})
            return response(200, {"username": username, "score": score})
        except Exception as e:
            logger.error(f"Score submit failed: {e}")
            return response(500, {"error": "Failed to submit score"})

    if path == "/leaderboard" and method == "GET":
        try:
            from datetime import datetime, timezone
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            lb = _dynamo.Table(LEADERBOARD_TABLE)
            result = lb.scan(FilterExpression=Attr("date").eq(today))
            items = sorted(result.get("Items", []), key=lambda x: int(x.get("score", 0)), reverse=True)
            return response(200, {"date": today, "leaderboard": [{"username": i["username"], "score": int(i["score"])} for i in items[:10]]})
        except Exception as e:
            logger.error(f"Leaderboard fetch failed: {e}")
            return response(500, {"error": "Failed to fetch leaderboard"})

    if path == "/decades" and method == "GET":
        decades = get_available_decades()
        return response(200, {"decades": decades})

    if path == "/movies-list" and method == "GET":
        try:
            obj = _s3.get_object(Bucket=S3_BUCKET, Key="static/movies-list.json")
            titles = json.loads(obj["Body"].read())
            return response(200, {"titles": titles, "count": len(titles)})
        except Exception as e:
            logger.error(f"Failed to load movies list: {e}")
            return response(500, {"error": "Could not load movies list"})

    if path == "/movies" and method == "GET":
        movies = get_all_movies()
        return response(200, {"movies": movies, "count": len(movies)})

    if path.startswith("/movies/decade/") and method == "GET":
        try:
            decade = int(path.split("/movies/decade/")[-1].rstrip("/"))
        except ValueError:
            return response(400, {"error": "Invalid decade. Use format: 1980, 1990, 2000, etc."})
        params = event.get("queryStringParameters") or {}
        exclude = [e.strip() for e in params.get("exclude", "").split(",") if e.strip()]
        movies = get_movies_by_decade(decade, exclude=exclude)
        if not movies:
            return response(404, {"error": f"No approved movies found for decade {decade}s"})
        return response(200, {"decade": decade, "count": len(movies), "movies": [enrich_with_urls(m) for m in movies]})

    if path == "/random-movie" and method == "GET":
        params = event.get("queryStringParameters") or {}
        exclude = [e.strip() for e in params.get("exclude", "").split(",") if e.strip()]
        movie = get_random_movie(exclude=exclude)
        if not movie:
            return response(404, {"error": "No approved movies found"})
        return response(200, enrich_with_urls(movie))

    if path.startswith("/movie/") and method == "GET":
        movie_id = path_params.get("id") or path.split("/movie/")[-1]
        movie = get_movie_by_id(movie_id)
        if not movie:
            return response(404, {"error": f"Movie '{movie_id}' not found"})
        return response(200, enrich_with_urls(movie))

    return response(404, {"error": "Not found"})
