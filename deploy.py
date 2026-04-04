"""
deploy.py — builds and deploys the Lambda container + SQS queue.
Run once to set up infrastructure, then use enqueue.py to process movies.

Usage: poetry run python3 deploy.py
"""

import json
import subprocess
import boto3
import logging

from config import AWS_REGION, S3_BUCKET, DYNAMO_TABLE

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AWS_ACCOUNT_ID = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
ECR_REPO = "movie-screencaps-pipeline"
LAMBDA_NAME = "movie-screencaps-pipeline"
SQS_QUEUE_NAME = "movie-screencaps-queue"
LAMBDA_ROLE_NAME = "movie-screencaps-lambda-role"
IMAGE_URI = f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/{ECR_REPO}:latest"

iam = boto3.client("iam", region_name=AWS_REGION)
ecr = boto3.client("ecr", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)


def create_iam_role() -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    try:
        role = iam.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Role for movie screencaps Lambda",
        )
        role_arn = role["Role"]["Arn"]
        logger.info(f"Created IAM role: {role_arn}")
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=LAMBDA_ROLE_NAME)["Role"]["Arn"]
        logger.info(f"IAM role already exists: {role_arn}")

    # Attach required policies
    policies = [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
        "arn:aws:iam::aws:policy/AmazonBedrockFullAccess",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
    ]
    for policy in policies:
        iam.attach_role_policy(RoleName=LAMBDA_ROLE_NAME, PolicyArn=policy)
    logger.info("Attached IAM policies")
    return role_arn


def create_ecr_repo():
    try:
        ecr.create_repository(repositoryName=ECR_REPO)
        logger.info(f"Created ECR repo: {ECR_REPO}")
    except ecr.exceptions.RepositoryAlreadyExistsException:
        logger.info(f"ECR repo already exists: {ECR_REPO}")


def build_and_push():
    logger.info("Building Docker image...")
    subprocess.run([
        "docker", "build",
        "--platform", "linux/arm64",
        "--provenance=false",
        "--no-cache",
        "-t", ECR_REPO, "."
    ], check=True)

    logger.info("Authenticating with ECR...")
    token = ecr.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    import base64
    user, pwd = base64.b64decode(token).decode().split(":")
    registry = f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
    subprocess.run(["docker", "login", "-u", user, "-p", pwd, registry], check=True)

    logger.info("Tagging and pushing image...")
    subprocess.run(["docker", "tag", f"{ECR_REPO}:latest", IMAGE_URI], check=True)
    subprocess.run(["docker", "push", IMAGE_URI], check=True)
    logger.info(f"Pushed: {IMAGE_URI}")


def create_sqs_queue() -> str:
    # Create DLQ first
    dlq_name = f"{SQS_QUEUE_NAME}-dlq"
    try:
        dlq = sqs.create_queue(QueueName=dlq_name)
        dlq_url = dlq["QueueUrl"]
        logger.info(f"Created DLQ: {dlq_url}")
    except sqs.exceptions.QueueNameExists:
        dlq_url = sqs.get_queue_url(QueueName=dlq_name)["QueueUrl"]
        logger.info(f"DLQ already exists: {dlq_url}")

    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # Create main queue with redrive policy — max 2 attempts before DLQ
    redrive = json.dumps({"maxReceiveCount": "2", "deadLetterTargetArn": dlq_arn})
    try:
        resp = sqs.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "900",
                "RedrivePolicy": redrive,
            },
        )
        queue_url = resp["QueueUrl"]
        logger.info(f"Created SQS queue: {queue_url}")
    except sqs.exceptions.QueueNameExists:
        queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
        # Update redrive policy on existing queue
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"RedrivePolicy": redrive},
        )
        logger.info(f"SQS queue already exists, updated redrive policy: {queue_url}")

    logger.info(f"Failed movies (after 2 attempts) will land in DLQ: {dlq_name}")
    return queue_url


def deploy_lambda(role_arn: str):
    import time
    time.sleep(10)  # Wait for IAM role to propagate

    try:
        lambda_client.create_function(
            FunctionName=LAMBDA_NAME,
            PackageType="Image",
            Code={"ImageUri": IMAGE_URI},
            Role=role_arn,
            Timeout=900,
            MemorySize=1024,
            Architectures=["arm64"],
            Environment={"Variables": {"APP_REGION": AWS_REGION}},
        )
        logger.info(f"Created Lambda: {LAMBDA_NAME}")
    except lambda_client.exceptions.ResourceConflictException:
        lambda_client.update_function_code(
            FunctionName=LAMBDA_NAME,
            ImageUri=IMAGE_URI,
        )
        logger.info(f"Updated Lambda: {LAMBDA_NAME}")

    # Set concurrency limit to 10
    lambda_client.put_function_concurrency(
        FunctionName=LAMBDA_NAME,
        ReservedConcurrentExecutions=10,
    )
    logger.info("Set Lambda concurrency to 10")


def attach_sqs_trigger(queue_url: str):
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=LAMBDA_NAME,
            BatchSize=1,  # One movie per Lambda invocation
            Enabled=True,
        )
        logger.info("Attached SQS trigger to Lambda")
    except lambda_client.exceptions.ResourceConflictException:
        logger.info("SQS trigger already attached")


if __name__ == "__main__":
    logger.info("=== Deploying movie screencaps pipeline ===")
    role_arn = create_iam_role()
    create_ecr_repo()
    build_and_push()
    queue_url = create_sqs_queue()
    deploy_lambda(role_arn)
    attach_sqs_trigger(queue_url)
    logger.info(f"\n✅ Done. Queue URL: {queue_url}")
    logger.info(f"Run: poetry run python3 enqueue.py to start processing movies")
