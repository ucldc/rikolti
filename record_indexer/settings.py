import os

from boto3 import Session
from dotenv import load_dotenv
from opensearchpy import AWSV4SignerAuth

load_dotenv()

def get_auth():
    credentials = Session().get_credentials()
    if not credentials:
        return False
    return AWSV4SignerAuth(
        credentials, os.environ.get("AWS_REGION", "us-west-2"))

ENDPOINT = os.environ.get("RIKOLTI_ES_ENDPOINT", False)
INDEX_RETENTION = os.environ.get("INDEX_RETENTION", 1)
