import json
import os
import boto3
import sys
import subprocess
import settings

from Fetcher import Fetcher, FetchError
from NuxeoFetcher import NuxeoFetcher
from OACFetcher import OACFetcher
from OAIFetcher import OAIFetcher

def get_fetcher(payload):
    harvest_type = payload.get('harvest_type')
    try:
        globals()[harvest_type]
    except KeyError:
        print(f"{ harvest_type } not imported")
        exit()

    if globals()[harvest_type] not in Fetcher.__subclasses__():
        print(f"{ harvest_type } not a subclass of Fetcher")
        exit()

    try:
        fetcher = globals()[harvest_type](payload)
    except NameError:
        print(f"bad harvest type: { harvest_type }")
        exit()

    return fetcher

# AWS Lambda entry point
def fetch_collection(payload, context):
    if settings.LOCAL_RUN:
        payload = json.loads(payload)

    fetcher = get_fetcher(payload)

    fetcher.fetch_page()
    next_page = fetcher.json()
    if next_page:
        if settings.LOCAL_RUN:
            subprocess.run([
                'python',
                'lambda_function.py',
                next_page.encode('utf-8')
            ])
        else:
            lambda_client = boto3.client('lambda', region_name="us-west-2",)
            lambda_client.invoke(
                FunctionName="fetch-metadata",
                InvocationType="Event",  # invoke asynchronously
                Payload=next_page.encode('utf-8')
            )

    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    fetch_collection(args.payload, {})
