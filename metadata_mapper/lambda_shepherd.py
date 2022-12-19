import json
import os
import boto3
import sys
import requests
from lambda_function import map_page
import settings


def get_collection(collection_id):
    collection = requests.get(
        f'https://registry.cdlib.org/api/v1/'
        f'rikolticollection/{collection_id}/?format=json'
    ).json()
    return collection


def check_for_missing_enrichments(collection):
    """Check for missing enrichments - used for development but
    could likely be removed in production?"""
    from mappers.mapper import Record
    from urllib.parse import urlparse

    not_yet_implemented = []
    collection_enrichments = (
        collection.get('rikolti__pre_enrichments', []) +
        collection.get('rikolti__enrichments')
    )
    for e_url in collection_enrichments:
        e_path = urlparse(e_url).path
        e_func_name = e_path.strip('/').replace('-', '_')
        if e_func_name not in dir(Record):
            not_yet_implemented.append(e_func_name)

    return not_yet_implemented


# {"collection_id": 26098, "source_type": "nuxeo"}
# {"collection_id": 26098, "source_type": "nuxeo"}
# AWS Lambda entry point
def map_collection(payload, context):
    if settings.LOCAL_RUN and isinstance(payload, str):
        payload = json.loads(payload)

    collection_id = payload.get('collection_id')
    collection = get_collection(collection_id)
    payload.update({'collection': collection})

    if not collection_id:
        print("ERROR ERROR ERROR")
        print('collection_id required')
        exit()

    missing_enrichments = check_for_missing_enrichments(collection)
    if len(missing_enrichments) > 0:
        print(f"[{collection_id}]: Missing enrichments: {missing_enrichments}")

    if settings.DATA_SRC == 'local':
        vernacular_path = settings.local_path(
            'vernacular_metadata', collection_id)
        page_list = [f for f in os.listdir(vernacular_path)
                     if os.path.isfile(os.path.join(vernacular_path, f))]
        count = 0
        for page in page_list:
            payload.update({'page_filename': page})
            return_val = map_page(json.dumps(payload), {})
            count += return_val['body']
        return {
            'statusCode': 200,
            'body': {
                'collection_id': collection_id,
                'missing_enrichments': missing_enrichments,
                'count': count,
            }
        }
    else:
        # JUST A SKETCH
        s3 = boto3.resource('s3')
        rikolti_bucket = s3.Bucket('rikolti')
        page_list = rikolti_bucket.objects.filter(
            Prefix=f'vernacular_metadata/{collection_id}')

        lambda_client = boto3.client('lambda', region_name="us-west-2",)
        for page in page_list:
            payload.update({'page_filename': page.key})
            lambda_client.invoke(
                FunctionName="map_metadata",
                InvocationType="Event",  # invoke asynchronously
                Payload=json.dumps(payload).encode('utf-8')
            )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    map_collection(args.payload, {})
