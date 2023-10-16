import argparse
import json
import os
import sys

import boto3

from urllib.parse import urlparse

from .indexer import index_records
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
        # TODO: instantiate one boto3 client and pass it around
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f'mapped_with_content/{collection_id}/'
        )
        file_list = [obj['Key'].split('/')[-1] for obj in response['Contents']]

    return file_list


def index_collection(collection_id):

    # each file contains metadata for a single record
    page_list = get_file_list(collection_id)

    for page in page_list:
        index_records(page, collection_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add collection data to OpenSearch index")
    parser.add_argument('collection_id', help='Registry collection ID')
    args = parser.parse_args(sys.argv[1:])
    index_collection(args.collection_id)
    sys.exit(0)
