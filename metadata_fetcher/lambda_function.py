import json
import boto3
import sys
import settings
import importlib
from Fetcher import Fetcher


def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        harvest_type, package="metadata_fetcher")
    fetcher_class = getattr(fetcher_module, harvest_type)
    if fetcher_class not in Fetcher.__subclasses__():
        print(f"{ harvest_type } not a subclass of Fetcher")
        exit()
    return fetcher_class


invocation_stack = None


# AWS Lambda entry point
def fetch_collection(payload, context):
    global invocation_stack
    if settings.LOCAL_RUN:
        payload = json.loads(payload)

    fetcher_class = import_fetcher(payload.get('harvest_type'))
    fetcher = fetcher_class(payload)

    fetcher.fetch_page()
    next_page = fetcher.json()
    if next_page:
        if settings.LOCAL_RUN:
            if invocation_stack is None:
                invocation_stack = [next_page.encode('utf-8')]
                while invocation_stack:
                    fetch_collection(invocation_stack.pop(), {})
            else:
                invocation_stack.append(next_page.encode('utf-8'))
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
