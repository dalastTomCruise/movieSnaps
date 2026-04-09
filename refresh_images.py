"""
refresh_images.py — updates images_to_show and presigned_urls for all approved movies.
Picks up to 10 random screencaps, generates 24h presigned URLs, stores in DynamoDB.
Runs nightly via EventBridge at 3am UTC.

Usage: poetry run python3 refresh_images.py
"""

import json
import logging
import random
import boto3
from boto3.dynamodb.conditions import Attr
from config import AWS_REGION, DYNAMO_TABLE, S3_BUCKET, IMAGES_TO_SHOW_CAP

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

URL_EXPIRY = 86400  # 24 hours


def refresh_all():
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    items = []
    resp = table.scan(FilterExpression=Attr("status").eq("approved"))
    items.extend(resp["Items"])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("status").eq("approved"),
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp["Items"])

    logger.info(f"Found {len(items)} approved movies")
    updated = 0
    skipped = 0

    for movie in items:
        caps = movie.get("movie_screen_caps", [])
        if not caps:
            skipped += 1
            continue

        # Split by has_people metadata (supports both old string format and new dict format)
        no_people = [c for c in caps if not (c.get("has_people") if isinstance(c, dict) else False)]
        with_people = [c for c in caps if (c.get("has_people") if isinstance(c, dict) else False)]

        # 80/20 ratio
        n_no_people = min(int(IMAGES_TO_SHOW_CAP * 0.8), len(no_people))
        n_with_people = min(IMAGES_TO_SHOW_CAP - n_no_people, len(with_people))
        if n_no_people + n_with_people < IMAGES_TO_SHOW_CAP:
            extra = IMAGES_TO_SHOW_CAP - n_no_people - n_with_people
            n_no_people = min(n_no_people + extra, len(no_people))

        selected = random.sample(no_people, n_no_people) + random.sample(with_people, n_with_people)
        random.shuffle(selected)
        selected_keys = [c["key"] if isinstance(c, dict) else c for c in selected]
        # Generate 24h presigned URLs
        presigned = []
        for key in selected_keys:
            try:
                url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET, "Key": key},
                    ExpiresIn=URL_EXPIRY,
                )
                presigned.append(url)
            except Exception as e:
                logger.warning(f"Failed presigned URL for {key}: {e}")

        # Hard mode: no people, no iconic scenes
        hard_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people") and not c.get("iconic_scene")]
        if not hard_pool:
            hard_pool = no_people  # fallback to just no-people
        hard_selected = random.sample(hard_pool, min(IMAGES_TO_SHOW_CAP, len(hard_pool)))
        random.shuffle(hard_selected)
        hard_keys = [c["key"] if isinstance(c, dict) else c for c in hard_selected]
        hard_presigned = []
        for key in hard_keys:
            try:
                url = s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=URL_EXPIRY)
                hard_presigned.append(url)
            except Exception as e:
                logger.warning(f"Failed hard presigned URL for {key}: {e}")
            except Exception as e:
                logger.warning(f"Failed presigned URL for {key}: {e}")

        table.update_item(
            Key={"movie_id": movie["movie_id"]},
            UpdateExpression="SET images_to_show = :keys, presigned_urls = :urls, hard_images_to_show = :hard, hard_presigned_urls = :hard_urls",
            ExpressionAttributeValues={
                ":keys": selected_keys,
                ":urls": presigned,
                ":hard": hard_keys,
                ":hard_urls": hard_presigned,
            },
        )
        updated += 1

    logger.info(f"Done. {updated} updated, {skipped} skipped (no caps)")
    return {"updated": updated, "skipped": skipped}


def handler(event, context):
    result = refresh_all()
    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    refresh_all()
