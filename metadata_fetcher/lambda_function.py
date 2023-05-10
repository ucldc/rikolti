import json
import boto3
import sys
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
    if settings.LOCAL_RUN and isinstance(payload, str):
        payload = json.loads(payload)

    fetcher_class = import_fetcher(payload.get('harvest_type'))

    return_val = {'page': payload.get('write_page', 0), 'document_count': 0}
    try:
        fetcher = fetcher_class(payload)
        return_val['document_count'] = fetcher.fetch_page()
    except InvalidHarvestEndpoint as e:
        print(e)
        return_val.update({
            'status': 'error',
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        })
        return [return_val]

    next_page = fetcher.json()
    return_val.update({
        'status': 'success',
        'next_page': next_page
    })

    return_val = [return_val]

    if not json.loads(next_page).get('finished'):
        if settings.LOCAL_RUN:
            return_val.extend(fetch_collection(next_page, {}))
        else:
            lambda_client = boto3.client('lambda', region_name="us-west-2",)
            lambda_client.invoke(
                FunctionName="fetch_metadata",
                InvocationType="Event",  # invoke asynchronously
                Payload=next_page.encode('utf-8')
            )

    return return_val


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    fetch_collection(args.payload, {})
    sys.exit(0)
