"""
deploy_api.py — deploys the API Gateway + Lambda for the movie screencaps game.

Usage: poetry run python3 deploy_api.py
"""

import json
import logging
import zipfile
import io

import boto3

from config import AWS_REGION, S3_BUCKET, DYNAMO_TABLE

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AWS_ACCOUNT_ID = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
API_LAMBDA_NAME = "movie-screencaps-api"
API_LAMBDA_ROLE = "movie-screencaps-lambda-role"  # reuse existing role

iam = boto3.client("iam", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
apigw = boto3.client("apigateway", region_name=AWS_REGION)


def build_zip() -> bytes:
    """Bundle api_handler.py and config.py into a zip for Lambda."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("api_handler.py")
        zf.write("config.py")
    return buf.getvalue()


def deploy_lambda(role_arn: str) -> str:
    zip_bytes = build_zip()
    try:
        lambda_client.create_function(
            FunctionName=API_LAMBDA_NAME,
            Runtime="python3.13",
            Role=role_arn,
            Handler="api_handler.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
            MemorySize=256,
            Environment={"Variables": {"APP_REGION": AWS_REGION}},
        )
        logger.info(f"Created Lambda: {API_LAMBDA_NAME}")
    except lambda_client.exceptions.ResourceConflictException:
        lambda_client.update_function_code(
            FunctionName=API_LAMBDA_NAME,
            ZipFile=zip_bytes,
        )
        logger.info(f"Updated Lambda: {API_LAMBDA_NAME}")

    fn = lambda_client.get_function(FunctionName=API_LAMBDA_NAME)
    return fn["Configuration"]["FunctionArn"]


def deploy_api(lambda_arn: str) -> str:
    # Check if API already exists
    apis = apigw.get_rest_apis()["items"]
    api = next((a for a in apis if a["name"] == "movie-screencaps-api"), None)

    if api:
        api_id = api["id"]
        logger.info(f"API already exists: {api_id}")
    else:
        api = apigw.create_rest_api(
            name="movie-screencaps-api",
            description="Movie screencaps game API",
        )
        api_id = api["id"]
        logger.info(f"Created API: {api_id}")

    root_id = apigw.get_resources(restApiId=api_id)["items"][0]["id"]

    def ensure_resource(parent_id: str, path_part: str) -> str:
        resources = apigw.get_resources(restApiId=api_id)["items"]
        existing = next((r for r in resources if r.get("pathPart") == path_part and r.get("parentId") == parent_id), None)
        if existing:
            return existing["id"]
        r = apigw.create_resource(restApiId=api_id, parentId=parent_id, pathPart=path_part)
        return r["id"]

    def add_method(resource_id: str, http_method: str, lambda_arn: str):
        try:
            apigw.put_method(
                restApiId=api_id, resourceId=resource_id,
                httpMethod=http_method, authorizationType="NONE",
            )
        except apigw.exceptions.ConflictException:
            pass
        uri = f"arn:aws:apigateway:{AWS_REGION}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations"
        apigw.put_integration(
            restApiId=api_id, resourceId=resource_id,
            httpMethod=http_method, type="AWS_PROXY",
            integrationHttpMethod="POST", uri=uri,
        )

    # /random-movie
    random_id = ensure_resource(root_id, "random-movie")
    add_method(random_id, "GET", lambda_arn)
    add_method(random_id, "OPTIONS", lambda_arn)

    # /movie/{id}
    movie_id = ensure_resource(root_id, "movie")
    id_resource = ensure_resource(movie_id, "{id}")
    add_method(id_resource, "GET", lambda_arn)
    add_method(id_resource, "OPTIONS", lambda_arn)

    # Grant API Gateway permission to invoke Lambda
    try:
        lambda_client.add_permission(
            FunctionName=API_LAMBDA_NAME,
            StatementId="apigw-invoke",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{AWS_REGION}:{AWS_ACCOUNT_ID}:{api_id}/*/*",
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass

    # Deploy
    apigw.create_deployment(restApiId=api_id, stageName="prod")
    logger.info("Deployed API to 'prod' stage")

    return f"https://{api_id}.execute-api.{AWS_REGION}.amazonaws.com/prod"


if __name__ == "__main__":
    role_arn = iam.get_role(RoleName=API_LAMBDA_ROLE)["Role"]["Arn"]
    lambda_arn = deploy_lambda(role_arn)
    base_url = deploy_api(lambda_arn)
    logger.info(f"\n✅ API deployed!")
    logger.info(f"  GET {base_url}/random-movie")
    logger.info(f"  GET {base_url}/movie/{{movie_id}}")
