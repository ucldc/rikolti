import json
import boto3
import sys
import subprocess
import settings
import importlib
from fetchers.Fetcher import Fetcher, InvalidHarvestEndpoint


def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        f"fetchers.{harvest_type}_fetcher", package="metadata_fetcher")
    fetcher_module_words = harvest_type.split('_')
    class_type = ''.join([word.capitalize() for word in fetcher_module_words])
    fetcher_class = getattr(fetcher_module, f"{class_type}Fetcher")
    if fetcher_class not in Fetcher.__subclasses__():
        raise Exception(
            f"Fetcher class {fetcher_class} not a subclass of Fetcher")
    return fetcher_class


# AWS Lambda entry point
def fetch_collection(payload, context):
    if settings.LOCAL_RUN:
        payload = json.loads(payload)

    fetcher_class = import_fetcher(payload.get('harvest_type'))

    try:
        fetcher = fetcher_class(payload)
        fetcher.fetch_page()
    except InvalidHarvestEndpoint as e:
        print(e)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        }

    next_page = fetcher.json()
    if not json.loads(next_page).get('finished'):
        if settings.LOCAL_RUN:
            subprocess.run([
                'python',
                'lambda_function.py',
                next_page.encode('utf-8')
            ])
        else:
            lambda_client = boto3.client('lambda', region_name="us-west-2",)
            lambda_client.invoke(
                FunctionName="fetch_metadata",
                InvocationType="Event",  # invoke asynchronously
                Payload=next_page.encode('utf-8')
            )

    return {
        'statusCode': 200,
        'body': next_page
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    fetch_collection(args.payload, {})
    sys.exit(0)
