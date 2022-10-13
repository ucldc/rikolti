import json
import os
import boto3
import sys
import subprocess
import requests
from lambda_function import lambda_handler

DEBUG = os.environ.get('DEBUG', False)


def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir, 
        'rikolti_bucket', 
        folder, 
        str(collection_id),
    ])
    return local_path

def get_collection_enrichments(collection_id):
    collection = requests.get(
        f'https://registry.cdlib.org/api/v1/'
        f'rikolticollection/{collection_id}/?format=json'
    ).json()
    return {
        'pre_mapping': collection.get('rikolti__pre_mapping'),
        'mapper_type': collection.get('rikolti__mapper_type'),
        'enrichments': collection.get('rikolti__enrichments'),
    }

# {"collection_id": 26098, "source_type": "nuxeo"}
# {"collection_id": 26098, "source_type": "nuxeo"}
def lambda_shepherd(payload, context):
    if DEBUG:
        payload = json.loads(payload)

    collection_id = payload.get('collection_id')
    payload.update({
        'enrichments': get_collection_enrichments(collection_id)})
    if not collection_id:
        print("ERROR ERROR ERROR")
        print('collection_id required')
        exit()
    
    if DEBUG:
        vernacular_path = local_path('vernacular_metadata', collection_id)
        page_list = [f for f in os.listdir(vernacular_path) 
                    if os.path.isfile(os.path.join(vernacular_path, f))]
        for page in page_list:
            payload.update({'page_filename': page})
            lambda_handler(json.dumps(payload), {})
    else:
        # SKETCHY
        s3 = boto3.resource('s3')
        rikolti_bucket = s3.Bucket('rikolti')
        page_list = rikolti_bucket.objects.filter(
            Prefix=f'vernacular_metadata/{self.collection_id}')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    lambda_shepherd(args.payload, {})
