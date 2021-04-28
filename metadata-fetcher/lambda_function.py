import json
import os

DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
    import boto3

from Fetcher import FetchError
from NuxeoFetcher import NuxeoFetcher
# from OAIFetcher import OAIFetcher
# from OACFetcher import OACFetcher

instantiate = {
    'nuxeo': NuxeoFetcher,
    # 'oai': OAIFetcher,
    # 'oac': OACFetcher,
}

def invoke_next(payload):
    print(f"NEW INVOCATION")
    print(payload)
    if (DEBUG):
        timer(json.loads(payload))
    else:
        # time to spawn a new lambda
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html
        lambda_client = boto3.client('lambda', region_name="us-west-2",)
        lambda_client.invoke(
          FunctionName="metadata-fetch",
          InvocationType="Event", #invoke asynchronously
          Payload=payload.encode('utf-8')
        )


def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)
    
    harvest_type = payload.get('harvest_type')
    if not harvest_type or not harvest_type in instantiate:
        print(f"bad harvest type: { payload.get('harvest_type') }")
        exit()


    fetcher = instantiate[harvest_type](payload)
    while fetcher.build_fetch_request():
        fetcher.fetchPage()
        invoke_next(fetcher.json())


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
