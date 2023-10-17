import argparse
from datetime import datetime
import json
import os
import sys
from urllib.parse import urlparse

import boto3

from .indexer import add_index_to_alias, add_page, remove_indices_from_alias
from . import settings


def get_file_list(collection_id):
    file_list = []

    if settings.DATA_SRC["STORE"] == 'file':
        path = settings.local_path('mapped_with_content', collection_id)
        try:
            file_list = [f for f in os.listdir(path)
                            if os.path.isfile(os.path.join(path, f))]
        except FileNotFoundError as e:
            print(f"{e} - have you run content fetcher for {collection_id}?")
    else:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f'mapped_with_content/{collection_id}/'
        )
        file_list = [obj['Key'].split('/')[-1] for obj in response['Contents']]

    return file_list


def add_collection_to_index(collection_id):
    page_list = get_file_list(collection_id)

    datetime_string = datetime.today().strftime('%Y%m%d%H%M%S')
    index_name = f"rikolti-{collection_id}-{datetime_string}"

    remove_indices_from_alias(collection_id, 'rikolti-stg')

    for page in page_list:
        add_page(page, collection_id, index_name)

    add_index_to_alias(index_name, "rikolti-stg", collection_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add collection data to OpenSearch index")
    parser.add_argument('collection_id', help='Registry collection ID')
    args = parser.parse_args(sys.argv[1:])
    add_collection_to_index(args.collection_id)
    sys.exit(0)
