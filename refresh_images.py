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

        selected_keys = random.sample(caps, min(IMAGES_TO_SHOW_CAP, len(caps)))
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

        table.update_item(
            Key={"movie_id": movie["movie_id"]},
            UpdateExpression="SET images_to_show = :keys, presigned_urls = :urls",
            ExpressionAttributeValues={
                ":keys": selected_keys,
                ":urls": presigned,
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
