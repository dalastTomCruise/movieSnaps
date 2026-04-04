# storage.py — S3 uploads and DynamoDB read/write

import logging
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError

from config import AWS_REGION, DYNAMO_TABLE, S3_BUCKET, USER_AGENT

logger = logging.getLogger(__name__)

_s3 = boto3.client("s3", region_name=AWS_REGION)
_dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def ensure_bucket() -> None:
    """Create the S3 bucket if it doesn't exist."""
    try:
        _s3.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket {S3_BUCKET!r} already exists")
    except ClientError:
        logger.info(f"Creating bucket {S3_BUCKET!r}")
        if AWS_REGION == "us-east-1":
            _s3.create_bucket(Bucket=S3_BUCKET)
        else:
            _s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )


def upload_image(image_url: str, movie_id: str, page: int, index: int) -> str | None:
    """Download an image and upload it to S3. Returns the S3 key or None on failure."""
    key = f"movies/{movie_id}/page{page:02d}_img{index:03d}.jpg"
    try:
        resp = requests.get(
            image_url,
            headers={
                "User-Agent": USER_AGENT,
                "Referer": "https://movie-screencaps.com/",
            },
            timeout=15,
        )
        resp.raise_for_status()
        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=resp.content,
            ContentType=resp.headers.get("Content-Type", "image/jpeg"),
        )
        logger.info(f"Uploaded {key}")
        return key
    except Exception as e:
        logger.warning(f"Failed to upload {image_url}: {e}")
        return None


def list_images(movie_id: str) -> list[str]:
    """List all S3 keys under movies/{movie_id}/."""
    prefix = f"movies/{movie_id}/"
    paginator = _s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_presigned_url(key: str, expires: int = 3600) -> str:
    """Generate a presigned URL for Claude to access an S3 image."""
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

def ensure_table() -> None:
    """Create the DynamoDB table if it doesn't exist."""
    client = boto3.client("dynamodb", region_name=AWS_REGION)
    existing = client.list_tables()["TableNames"]
    if DYNAMO_TABLE in existing:
        logger.info(f"Table {DYNAMO_TABLE!r} already exists")
        return
    logger.info(f"Creating DynamoDB table {DYNAMO_TABLE!r}")
    client.create_table(
        TableName=DYNAMO_TABLE,
        KeySchema=[{"AttributeName": "movie_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "movie_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=DYNAMO_TABLE)


def save_movie(movie_id: str, data: dict) -> None:
    """Write or update a movie record in DynamoDB."""
    table = _dynamo.Table(DYNAMO_TABLE)
    data["movie_id"] = movie_id
    data.setdefault("scraped_at", datetime.now(timezone.utc).isoformat())
    table.put_item(Item=data)
    logger.info(f"Saved movie {movie_id!r} to DynamoDB")


def get_movie(movie_id: str) -> dict | None:
    """Fetch a movie record from DynamoDB."""
    table = _dynamo.Table(DYNAMO_TABLE)
    resp = table.get_item(Key={"movie_id": movie_id})
    return resp.get("Item")


def update_screencaps(movie_id: str, screencap_keys: list[str]) -> None:
    """Update the movie_screen_caps field on an existing record."""
    table = _dynamo.Table(DYNAMO_TABLE)
    table.update_item(
        Key={"movie_id": movie_id},
        UpdateExpression="SET movie_screen_caps = :caps, #st = :status",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":caps": screencap_keys,
            ":status": "approved",
        },
    )
    logger.info(f"Updated screencaps for {movie_id!r}")
