"""
Lambda handler — entry point for movie screencap pipeline.
Triggered by SQS messages with movie title payloads.
"""

import json
import logging
import traceback

from pipeline import run

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    SQS event format:
    {
        "Records": [
            {
                "body": "{\"title\": \"Inception\", \"pages\": 10}"
            }
        ]
    }
    """
    results = []
    failures = []

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        title = body.get("title")
        url = body.get("url")
        pages = body.get("pages", 10)

        if not title and not url:
            logger.warning(f"Skipping record with no title or url: {body}")
            continue

        label = url or title
        logger.info(f"Processing: {label!r} ({pages} pages)")
        try:
            result = run(title or "", pages=pages, url=url)
            logger.info(f"Done: {label!r} → {len(result.get('movie_screen_caps', []))} screencaps")
            results.append(result)
        except Exception as e:
            logger.error(f"Failed: {label!r} — {e}\n{traceback.format_exc()}")
            failures.append({"label": label, "error": str(e)})
            raise

    return {
        "statusCode": 200,
        "processed": len(results),
        "failed": len(failures),
        "results": results,
    }
