"""
deploy_refresh.py — deploys the nightly images_to_show refresh Lambda + EventBridge schedule.

Usage: poetry run python3 deploy_refresh.py
"""

import io
import json
import logging
import zipfile

import boto3

from config import AWS_REGION

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LAMBDA_NAME = "movie-screencaps-refresh"
ROLE_NAME = "movie-screencaps-lambda-role"  # reuse existing role
SCHEDULE = "cron(0 3 * * ? *)"  # 3am UTC nightly

iam = boto3.client("iam", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
events = boto3.client("events", region_name=AWS_REGION)
AWS_ACCOUNT_ID = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]


def build_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("refresh_images.py")
        zf.write("config.py")
    return buf.getvalue()


def deploy_lambda() -> str:
    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    zip_bytes = build_zip()

    try:
        lambda_client.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.13",
            Role=role_arn,
            Handler="refresh_images.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=300,
            MemorySize=256,
            Environment={"Variables": {"APP_REGION": AWS_REGION}},
        )
        logger.info(f"Created Lambda: {LAMBDA_NAME}")
    except lambda_client.exceptions.ResourceConflictException:
        lambda_client.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        logger.info(f"Updated Lambda: {LAMBDA_NAME}")

    fn = lambda_client.get_function(FunctionName=LAMBDA_NAME)
    return fn["Configuration"]["FunctionArn"]


def deploy_schedule(lambda_arn: str):
    rule_name = "movie-screencaps-nightly-refresh"

    # Create/update EventBridge rule
    events.put_rule(
        Name=rule_name,
        ScheduleExpression=SCHEDULE,
        State="ENABLED",
        Description="Nightly refresh of images_to_show for all movies",
    )
    logger.info(f"EventBridge rule set: {SCHEDULE}")

    # Grant EventBridge permission to invoke Lambda
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="eventbridge-nightly",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{AWS_REGION}:{AWS_ACCOUNT_ID}:rule/{rule_name}",
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass

    # Add Lambda as target
    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "refresh-lambda", "Arn": lambda_arn}],
    )
    logger.info(f"Lambda target attached to rule")


if __name__ == "__main__":
    lambda_arn = deploy_lambda()
    deploy_schedule(lambda_arn)
    logger.info(f"\n✅ Refresh service deployed — runs nightly at 3am UTC")
    logger.info(f"Run manually: poetry run python3 refresh_images.py")
