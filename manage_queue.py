"""
manage_queue.py — helper to manage the movie processing queue and deployments.

Usage:
  poetry run python3 manage_queue.py status              # check queue + trigger state
  poetry run python3 manage_queue.py enable               # enable Lambda trigger
  poetry run python3 manage_queue.py disable              # disable Lambda trigger
  poetry run python3 manage_queue.py purge                # purge all messages
  poetry run python3 manage_queue.py enqueue 50           # enqueue 50 movies with fewest images
  poetry run python3 manage_queue.py enqueue 50 --all     # enqueue 50 unprocessed from available_movies.txt
  poetry run python3 manage_queue.py force on             # set FORCE_REPROCESS on Lambda
  poetry run python3 manage_queue.py force off            # remove FORCE_REPROCESS from Lambda
  poetry run python3 manage_queue.py push <movie_id>      # push local output/<movie_id>/ images to S3 + DynamoDB
  poetry run python3 manage_queue.py refresh <movie_id>   # refresh images_to_show for a single movie
  poetry run python3 manage_queue.py deploy-api           # redeploy the API Lambda
  poetry run python3 manage_queue.py deploy-refresh       # redeploy the refresh Lambda
"""

import os
import sys
import random
import json
import boto3
from config import AWS_REGION, DYNAMO_TABLE
from boto3.dynamodb.conditions import Attr

TRIGGER_UUID = "6d228780-3ca3-4c45-889e-3164f469cd7a"
LAMBDA_NAME = "movie-screencaps-pipeline"
QUEUE_NAME = "movie-screencaps-queue"

sqs = boto3.client("sqs", region_name=AWS_REGION)
lc = boto3.client("lambda", region_name=AWS_REGION)
dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)


def get_queue_url():
    return sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]


def status():
    queue_url = get_queue_url()
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    m = lc.list_event_source_mappings(FunctionName=LAMBDA_NAME)["EventSourceMappings"][0]
    config = lc.get_function_configuration(FunctionName=LAMBDA_NAME)
    force = config.get("Environment", {}).get("Variables", {}).get("FORCE_REPROCESS", "off")
    print(f"Trigger: {m['State']}")
    print(f"Queue visible: {attrs['ApproximateNumberOfMessages']}")
    print(f"Queue in-flight: {attrs['ApproximateNumberOfMessagesNotVisible']}")
    print(f"FORCE_REPROCESS: {force}")


def enable():
    lc.update_event_source_mapping(UUID=TRIGGER_UUID, Enabled=True)
    print("Trigger enabled")


def disable():
    lc.update_event_source_mapping(UUID=TRIGGER_UUID, Enabled=False)
    print("Trigger disabled")


def purge():
    sqs.purge_queue(QueueUrl=get_queue_url())
    print("Queue purged")


def force(state):
    config = lc.get_function_configuration(FunctionName=LAMBDA_NAME)
    env = config.get("Environment", {}).get("Variables", {})
    if state == "on":
        env["FORCE_REPROCESS"] = "1"
    else:
        env.pop("FORCE_REPROCESS", None)
    lc.update_function_configuration(FunctionName=LAMBDA_NAME, Environment={"Variables": env})
    print(f"FORCE_REPROCESS {'enabled' if state == 'on' else 'disabled'}")


def enqueue(n, from_available=False):
    queue_url = get_queue_url()
    table = dynamo.Table(DYNAMO_TABLE)

    if from_available:
        # Enqueue unprocessed movies from available_movies.txt
        import random
        s3 = boto3.client("s3", region_name=AWS_REGION)
        paginator = s3.get_paginator("list_objects_v2")
        processed = set()
        for page in paginator.paginate(Bucket="movie-screencaps-game", Prefix="movies/", Delimiter="/"):
            for prefix in page.get("CommonPrefixes", []):
                processed.add(prefix["Prefix"].split("/")[1])

        lines = [l.strip().split(",")[0] for l in open("available_movies.txt") if l.strip()]
        unprocessed = [url for url in lines if url.rstrip("/").split("/")[-1] not in processed]
        picks = random.sample(unprocessed, min(n, len(unprocessed)))
        for url in picks:
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"url": url, "title": ""}))
        print(f"Enqueued {len(picks)} unprocessed movies")
    else:
        # Enqueue movies with fewest images
        resp = table.scan(FilterExpression=Attr("status").eq("approved"))
        under = [m for m in resp["Items"] if len(m.get("movie_screen_caps", [])) < 10 and m.get("source_url")]
        under.sort(key=lambda x: len(x.get("movie_screen_caps", [])))
        picks = under[:n]
        for m in picks:
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"url": m["source_url"], "title": ""}))
        print(f"Enqueued {len(picks)} movies (fewest images first)")


def push(movie_id):
    """Push local output/<movie_id>/ images to S3 and update DynamoDB with metadata."""
    from config import S3_BUCKET, IMAGES_TO_SHOW_CAP
    from agent import get_similar_movies

    s3 = boto3.client("s3", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)
    local_dir = f"output/{movie_id}"

    if not os.path.exists(local_dir):
        print(f"No local output found at {local_dir}")
        return

    prefix = f"movies/{movie_id}/"

    # Load metadata if available
    meta_path = f"{local_dir}/metadata.json"
    image_meta = []
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            image_meta = json.load(f)
        print(f"Loaded metadata for {len(image_meta)} images")
    else:
        print("Warning: no metadata.json found — tags will default to false")

    meta_by_key = {m["key"]: m for m in image_meta}

    # Delete existing in S3
    existing = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    old_keys = [o["Key"] for o in existing.get("Contents", [])]
    if old_keys:
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": [{"Key": k} for k in old_keys]})
        print(f"Deleted {len(old_keys)} existing images")

    # Upload new
    files = sorted(f for f in os.listdir(local_dir) if f.endswith((".jpg", ".jpeg", ".png")))
    new_caps = []
    for fname in files:
        key = f"{prefix}{fname}"
        with open(f"{local_dir}/{fname}", "rb") as f:
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=f.read(), ContentType="image/jpeg")
        meta = meta_by_key.get(key, {"key": key, "has_people": False, "has_cast": False, "iconic_scene": False})
        meta["key"] = key
        new_caps.append(meta)
    print(f"Uploaded {len(new_caps)} images")

    # Get existing item for metadata
    item = table.get_item(Key={"movie_id": movie_id}).get("Item", {})
    similar = get_similar_movies(item) if not item.get("similar_movies") else item["similar_movies"]

    # Update DynamoDB with full metadata
    table.update_item(
        Key={"movie_id": movie_id},
        UpdateExpression="SET movie_screen_caps = :k, #s = :s, similar_movies = :sim",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":k": new_caps, ":s": "approved", ":sim": similar},
    )

    # images_to_show + hard mode + presigned
    # images_to_show: 1 cast + 1 extras + 8 no-people
    cast_pool = [c for c in new_caps if c.get("has_cast")]
    extras_pool = [c for c in new_caps if c.get("has_people") and not c.get("has_cast")]
    empty_pool = [c for c in new_caps if not c.get("has_people")]

    selected = []
    if cast_pool:
        selected += random.sample(cast_pool, min(1, len(cast_pool)))
    if extras_pool:
        selected += random.sample(extras_pool, min(1, len(extras_pool)))
    remaining = IMAGES_TO_SHOW_CAP - len(selected)
    if empty_pool:
        selected += random.sample(empty_pool, min(remaining, len(empty_pool)))
    # Fill any remaining from extras if not enough empty
    if len(selected) < IMAGES_TO_SHOW_CAP and extras_pool:
        already = {id(c) for c in selected}
        extras_left = [c for c in extras_pool if id(c) not in already]
        selected += random.sample(extras_left, min(IMAGES_TO_SHOW_CAP - len(selected), len(extras_left)))
    random.shuffle(selected)
    selected_keys = [c["key"] for c in selected]
    presigned = [s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in selected_keys]

    # Hard mode: no people, no iconic scenes
    hard_pool = [c for c in new_caps if not c.get("has_people") and not c.get("iconic_scene")]
    if not hard_pool:
        hard_pool = empty_pool if empty_pool else new_caps
    hard_selected = random.sample(hard_pool, min(IMAGES_TO_SHOW_CAP, len(hard_pool)))
    hard_keys = [c["key"] for c in hard_selected]
    hard_presigned = [s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in hard_keys]

    table.update_item(
        Key={"movie_id": movie_id},
        UpdateExpression="SET images_to_show = :k, presigned_urls = :u, hard_images_to_show = :hk, hard_presigned_urls = :hu",
        ExpressionAttributeValues={":k": selected_keys, ":u": presigned, ":hk": hard_keys, ":hu": hard_presigned},
    )

    n_cast = sum(1 for c in new_caps if c.get("has_cast"))
    n_ppl_total = sum(1 for c in new_caps if c.get("has_people") and not c.get("has_cast"))
    n_empty = sum(1 for c in new_caps if not c.get("has_people"))
    n_iconic = sum(1 for c in new_caps if c.get("iconic_scene"))
    print(f"Done — 🎭 cast: {n_cast} | 👤 extras: {n_ppl_total} | 🏠 empty: {n_empty} | ⭐ iconic: {n_iconic}")
    print(f"images_to_show: {len(selected_keys)} | hard: {len(hard_keys)} | similar: {similar}")


def refresh_movie(movie_id):
    """Refresh images_to_show and hard_images_to_show for a single movie."""
    from config import S3_BUCKET, IMAGES_TO_SHOW_CAP

    s3 = boto3.client("s3", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)
    item = table.get_item(Key={"movie_id": movie_id}).get("Item", {})
    caps = item.get("movie_screen_caps", [])
    if not caps:
        print(f"No screencaps for {movie_id}")
        return

    cast_pool = [c for c in caps if isinstance(c, dict) and c.get("has_cast")]
    extras_pool = [c for c in caps if isinstance(c, dict) and c.get("has_people") and not c.get("has_cast")]
    empty_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people")]
    # Fallback for old string format
    if not cast_pool and not extras_pool and not empty_pool:
        empty_pool = [{"key": c} if isinstance(c, str) else c for c in caps]

    selected = []
    if cast_pool:
        selected += random.sample(cast_pool, min(1, len(cast_pool)))
    if extras_pool:
        selected += random.sample(extras_pool, min(1, len(extras_pool)))
    remaining = IMAGES_TO_SHOW_CAP - len(selected)
    if empty_pool:
        selected += random.sample(empty_pool, min(remaining, len(empty_pool)))
    random.shuffle(selected)

    selected_keys = [c["key"] if isinstance(c, dict) else c for c in selected]
    presigned = [s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in selected_keys]

    hard_pool = [c for c in caps if isinstance(c, dict) and not c.get("has_people") and not c.get("iconic_scene")]
    if not hard_pool:
        hard_pool = empty_pool
    hard_selected = random.sample(hard_pool, min(IMAGES_TO_SHOW_CAP, len(hard_pool)))
    hard_keys = [c["key"] if isinstance(c, dict) else c for c in hard_selected]
    hard_presigned = [s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": k}, ExpiresIn=86400) for k in hard_keys]

    table.update_item(
        Key={"movie_id": movie_id},
        UpdateExpression="SET images_to_show = :k, presigned_urls = :u, hard_images_to_show = :hk, hard_presigned_urls = :hu",
        ExpressionAttributeValues={":k": selected_keys, ":u": presigned, ":hk": hard_keys, ":hu": hard_presigned},
    )
    print(f"Refreshed {movie_id} — show: {len(selected_keys)} (1 cast + 1 extras + {len(selected_keys)-2} empty) | hard: {len(hard_keys)}")


def deploy_api():
    """Redeploy the API Lambda with latest api_handler.py + config.py."""
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("api_handler.py")
        zf.write("config.py")
    lc.update_function_code(FunctionName="movie-screencaps-api", ZipFile=buf.getvalue())
    print("API Lambda updated")


def deploy_refresh_lambda():
    """Redeploy the refresh Lambda with latest refresh_images.py + config.py."""
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("refresh_images.py")
        zf.write("config.py")
    lc.update_function_code(FunctionName="movie-screencaps-refresh", ZipFile=buf.getvalue())
    print("Refresh Lambda updated")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "status":
        status()
    elif args[0] == "enable":
        enable()
    elif args[0] == "disable":
        disable()
    elif args[0] == "purge":
        purge()
    elif args[0] == "force":
        force(args[1] if len(args) > 1 else "on")
    elif args[0] == "enqueue":
        n = int(args[1]) if len(args) > 1 else 50
        from_available = "--all" in args
        enqueue(n, from_available)
    elif args[0] == "push":
        if len(args) < 2:
            print("Usage: manage_queue.py push <movie_id>")
        else:
            push(args[1])
    elif args[0] == "refresh":
        if len(args) < 2:
            print("Usage: manage_queue.py refresh <movie_id>")
        else:
            refresh_movie(args[1])
    elif args[0] == "deploy-api":
        deploy_api()
    elif args[0] == "deploy-refresh":
        deploy_refresh_lambda()
    else:
        print(__doc__)
