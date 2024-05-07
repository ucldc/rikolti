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
if ENDPOINT:
    ENDPOINT = ENDPOINT.rstrip("/")
INDEX_RETENTION = os.environ.get("INDEX_RETENTION", 0)
STAGE_ALIAS = os.environ.get("RIKOLTI_ES_STAGE_ALIAS")
STAGE_ALIAS_COMBINED = os.environ.get("RIKOLTI_ES_ALIAS_STAGE_COMBINED") # we only need this during the transition from one collection per index --> all collections in a single combined index
