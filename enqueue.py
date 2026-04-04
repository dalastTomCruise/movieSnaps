"""
enqueue.py — sends all movies to the SQS queue for Lambda processing.

Usage: poetry run python3 enqueue.py
"""

import json
import logging
import boto3

from config import AWS_REGION
from movies import MOVIES

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQS_QUEUE_NAME = "movie-screencaps-queue"
DEFAULT_PAGES = 10

sqs = boto3.client("sqs", region_name=AWS_REGION)


def enqueue_movies(movies: list[str], pages: int = DEFAULT_PAGES):
    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    logger.info(f"Enqueueing {len(movies)} movies to {queue_url}")

    for title in movies:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"title": title, "pages": pages}),
        )
        logger.info(f"  Queued: {title!r}")

    logger.info(f"\n✅ {len(movies)} movies queued. Lambda will process up to 10 concurrently.")


if __name__ == "__main__":
    enqueue_movies(MOVIES)
