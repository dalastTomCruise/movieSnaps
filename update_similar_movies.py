"""
update_similar_movies.py — reads similar_movies.json and updates DynamoDB records
with a similar_movies attribute matched by title.

Usage: poetry run python3 update_similar_movies.py
"""

import json
import logging
import boto3
from config import AWS_REGION, DYNAMO_TABLE

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamo.Table(DYNAMO_TABLE)


def normalize(title: str) -> str:
    """Lowercase and strip year suffixes like (2014-1) for fuzzy matching."""
    import re
    title = title.lower().strip()
    title = re.sub(r"\s*\(\d{4}(-\d+)?\)\s*$", "", title)  # strip (2005) or (2005-1)
    return title


def load_similar_movies(path: str) -> dict:
    """Returns a dict of normalized_title -> list of similar movie strings."""
    with open(path) as f:
        data = json.load(f)
    mapping = {}
    for entry in data:
        key = normalize(entry["movie_title"])
        mapping[key] = entry["similar_movies"]
    return mapping


def fetch_all_movies() -> list[dict]:
    items = []
    resp = table.scan(ProjectionExpression="movie_id, title")
    items.extend(resp["Items"])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            ProjectionExpression="movie_id, title",
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp["Items"])
    return items


def run():
    similar = load_similar_movies("similar_movies.json")
    movies = fetch_all_movies()

    updated = 0
    skipped = 0

    for movie in movies:
        movie_id = movie["movie_id"]
        title = movie.get("title") or movie_id
        key = normalize(title)

        if key not in similar:
            logger.warning(f"No match for: '{title}' (key: '{key}')")
            skipped += 1
            continue

        table.update_item(
            Key={"movie_id": movie_id},
            UpdateExpression="SET similar_movies = :s",
            ExpressionAttributeValues={":s": similar[key]},
        )
        logger.info(f"Updated: {title}")
        updated += 1

    logger.info(f"\nDone. {updated} updated, {skipped} skipped.")


if __name__ == "__main__":
    run()
