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

from config import AWS_REGION, DYNAMO_TABLE, S3_BUCKET

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
    """
    # Use pre-generated presigned URLs if available
    if movie.get("presigned_urls"):
        movie["images_to_show"] = movie["presigned_urls"]
    else:
        # Fallback: generate presigned URLs on the fly from images_to_show keys
        show_keys = movie.get("images_to_show", [])
        show_urls = []
        for key in show_keys:
            try:
                url = _s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET, "Key": key},
                    ExpiresIn=expires,
                )
                show_urls.append(url)
            except Exception as e:
                logger.warning(f"Failed to generate presigned URL for {key}: {e}")
        movie["images_to_show"] = show_urls

    # Full screencap list (on-demand, for backwards compat)
    keys = movie.get("movie_screen_caps", [])
    urls = []
    for key in keys:
        try:
            url = _s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": S3_BUCKET, "Key": key},
                ExpiresIn=expires,
            )
            urls.append(url)
        except Exception as e:
            logger.warning(f"Failed to generate presigned URL for {key}: {e}")
    movie["image_urls"] = urls

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
        ProjectionExpression="movie_id, title, #yr",
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
        })
    return sorted(movies, key=lambda x: x["title"] or "")


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

    if path == "/decades" and method == "GET":
        decades = get_available_decades()
        return response(200, {"decades": decades})

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
