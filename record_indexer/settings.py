import os
import traceback
from pprint import pprint

import requests

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


def print_opensearch_error(r: requests.Response, url: str):
    direct_frame = traceback.extract_stack(limit=2)[0]
    print(f"ERROR @ {direct_frame.name} from {direct_frame.filename}")
    try:
        pprint(r.json())
    except requests.exceptions.JSONDecodeError:
        print(f"{r.status_code}: {url}")
        print(f"Response: {r.text}")


ENDPOINT = os.environ.get("RIKOLTI_ES_ENDPOINT", False)
if ENDPOINT:
    ENDPOINT = ENDPOINT.rstrip("/")
INDEX_RETENTION = os.environ.get("INDEX_RETENTION", 1)
