import os

from boto3 import Session
from dotenv import load_dotenv
from opensearchpy import AWSV4SignerAuth

load_dotenv()

es_user = os.environ.get("OPENSEARCH_USER")
es_pass = os.environ.get("OPENSEARCH_PASS")

def verify_certs():
    return not os.environ.get("OPENSEARCH_IGNORE_TLS", False)

def get_auth():
    if es_user and es_pass:
        return (es_user, es_pass)

    credentials = Session().get_credentials()
    if not credentials:
        return False
    return AWSV4SignerAuth(
        credentials, os.environ.get("AWS_REGION", "us-west-2"))


ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT", False)
if ENDPOINT:
    ENDPOINT = ENDPOINT.rstrip("/")
