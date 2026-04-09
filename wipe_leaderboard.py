"""Nightly wipe of the leaderboard table. Triggered by EventBridge at midnight UTC."""

import json
import logging
import boto3
from config import AWS_REGION, LEADERBOARD_TABLE

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def wipe():
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(LEADERBOARD_TABLE)
    resp = table.scan(ProjectionExpression="username")
    items = resp.get("Items", [])
    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"username": item["username"]})
    logger.info(f"Wiped {len(items)} leaderboard entries")
    return {"wiped": len(items)}


def handler(event, context):
    result = wipe()
    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    wipe()
