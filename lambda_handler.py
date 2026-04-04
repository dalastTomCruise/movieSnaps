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
        pages = body.get("pages", 10)

        if not title:
            logger.warning(f"Skipping record with no title: {body}")
            continue

        logger.info(f"Processing: {title!r} ({pages} pages)")
        try:
            result = run(title, pages=pages)
            logger.info(f"Done: {title!r} → {len(result.get('movie_screen_caps', []))} screencaps")
            results.append(result)
        except Exception as e:
            logger.error(f"Failed: {title!r} — {e}\n{traceback.format_exc()}")
            failures.append({"title": title, "error": str(e)})
            # Re-raise to let SQS retry the message
            raise

    return {
        "statusCode": 200,
        "processed": len(results),
        "failed": len(failures),
        "results": results,
    }
