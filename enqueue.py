"""
enqueue.py — sends movies to the SQS queue for Lambda processing.
Reads URLs from available_movies.txt (run discover_movies.py first).
Marks queued URLs as 'processed' so they won't be re-queued.

Usage: poetry run python3 enqueue.py [count]
  count: number of random movies to queue (default: 200)
"""

import json
import logging
import random
import sys

import boto3

from config import AWS_REGION

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQS_QUEUE_NAME = "movie-screencaps-queue"
DEFAULT_PAGES = 10
MOVIES_FILE = "available_movies.txt"


def load_movies(path: str = MOVIES_FILE) -> list[tuple[str, bool]]:
    """Returns list of (url, processed) tuples."""
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.endswith(",processed"):
                entries.append((line[: -len(",processed")], True))
            else:
                entries.append((line, False))
    return entries


def save_movies(entries: list[tuple[str, bool]], path: str = MOVIES_FILE):
    with open(path, "w") as f:
        for url, processed in entries:
            f.write(f"{url},processed\n" if processed else f"{url}\n")


def enqueue_movies(count: int = 200, pages: int = DEFAULT_PAGES):
    entries = load_movies()
    available = [(url, proc) for url, proc in entries if not proc and any(c.isdigit() for c in url)]
    logger.info(f"{len(available)} unprocessed URLs available")

    sample = random.sample(available, min(count, len(available)))

    sqs = boto3.client("sqs", region_name=AWS_REGION)
    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]

    sampled_urls = set()
    for url, _ in sample:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"url": url, "pages": pages}))
        sampled_urls.add(url)
        logger.info(f"  Queued: {url}")

    # Mark as processed in file
    updated = [(url, True if url in sampled_urls else proc) for url, proc in entries]
    save_movies(updated)

    logger.info(f"\n✅ {len(sample)} movies queued and marked as processed.")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    enqueue_movies(count)
