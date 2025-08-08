"""
Bootstraps environment variables and secrets into os.environ.
This module MUST be imported first in any Lambda handler to ensure proper configuration.
"""

import os
import boto3
from botocore.exceptions import ClientError

# Determine the AWS region (default to us-east-1)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)


def get_secret(secret_name: str) -> str:
    """Retrieve a secret string from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response.get("SecretString", "")
    except ClientError as e:
        raise RuntimeError(
            f"Unable to retrieve secret '{secret_name}': {e.response['Error']['Message']}"
        )
    except Exception as e:
        raise RuntimeError(
            f"Unexpected error retrieving secret '{secret_name}': {str(e)}"
        )


# Map of environment variables to secrets in AWS Secrets Manager
secrets_map = {
    "INTELLIGENCE_API_SECRET": "INTELLIGENCE_API_SECRET",
}

# Load secrets into os.environ, overwriting any existing values
for env_var, secret_name in secrets_map.items():
    secret_value = get_secret(secret_name)
    if secret_value:  # Only set if the secret was retrieved
        os.environ[env_var] = secret_value


# Apply default values for common Datadog config
defaults = {}

for key, default in defaults.items():
    os.environ[key] = default
