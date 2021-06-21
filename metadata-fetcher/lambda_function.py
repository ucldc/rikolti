import json
import os

DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
    import boto3

from Fetcher import FetchError
from NuxeoFetcher import NuxeoFetcher

def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)

    harvest_type = payload.get('harvest_type')
    try:
        fetcher = eval(harvest_type)(payload)
    except NameError:
        print(f"bad harvest type: { payload.get('harvest_type') }")
        exit()

    fetcher.fetchPage()
    next_page = fetcher.json()
    if next_page:
        if DEBUG:
            lambda_handler(next_page, {})
        
        lambda_client = boto3.client('lambda', region_name="us-west-2",)
        lambda_client.invoke(
            FunctionName="fetch-metadata",
            InvocationType="Event", #invoke asynchronously
            Payload=next_page.encode('utf-8')
        )

    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }
