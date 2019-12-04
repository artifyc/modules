import os
import os.path
import boto3

def login(
    access_key_id: str, secret_access_key: str, region: str = AWS_DEFAULT_REGION
) -> None:
    """sets environment variables for boto3."""
    os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_access_key
    os.environ["AWS_DEFAULT_REGION"] = region