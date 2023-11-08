import json
import os

import boto3

from . import settings
from .by_page import harvest_page_content


def get_mapped_pages(collection_id):
    page_list = []
    if settings.DATA_SRC['STORE'] == 'file':
        mapped_path = os.sep.join([
            settings.DATA_SRC["PATH"],
            str(collection_id),
            'mapped_metadata',
        ])
        try:
            page_list = [f for f in os.listdir(mapped_path)
                            if os.path.isfile(os.path.join(mapped_path, f))]
        except FileNotFoundError as e:
            print(f"{e} - have you mapped {collection_id}?")
    else:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            region_name=settings.AWS_REGION
        )
        response = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f'{collection_id}/mapped_metadata/'
        )
        page_list = [obj['Key'].split('/')[-1] for obj in response['Contents']]
    return page_list


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo"}
def harvest_collection(collection):
    if isinstance(collection, str):
        collection = json.loads(collection)

    collection_id = collection.get('collection_id')

    if not collection_id:
        print("ERROR ERROR ERROR\ncollection_id required")
        exit()

    page_list = get_mapped_pages(collection_id)

    print(f"[{collection_id}]: Harvesting content for {len(page_list)} pages")
    collection_stats = {}
    for page in page_list:
        collection.update({'page_filename': page})
        page_stats = harvest_page_content(**collection)

        # in some cases, value is int and in some cases, value is Counter
        # so we can't just collection_stats.get(key, 0) += value
        for key, value in page_stats.items():
            if key in collection_stats:
                collection_stats[key] += value
            else:
                collection_stats[key] = value

    collection_stats.update({
        'pages': len(page_list), 
        'collection_id': collection_id,
    })
    return collection_stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content by collection using mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('--nuxeo', action="store_true", help="Use Nuxeo auth")
    args = parser.parse_args()
    arguments = {
        'collection_id': args.collection_id,
    }
    if args.nuxeo:
        arguments['rikolti_mapper_type'] = 'nuxeo.nuxeo'
    print(harvest_collection(arguments))