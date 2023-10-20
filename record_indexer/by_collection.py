import argparse
from datetime import datetime
import json
import os
import sys
from urllib.parse import urlparse

import boto3

from .indexer import add_page, update_alias_for_collection_index
from . import settings



def get_page_list(collection_id):
    if settings.DATA_SRC["STORE"] == 'file':
        path = settings.local_path(collection_id, 'mapped_with_content')
        try:
            page_list = [f for f in os.listdir(path)
                            if os.path.isfile(os.path.join(path, f))]
        except FileNotFoundError as e:
            print(f"{e} - have you run content fetcher for {collection_id}?")
    else:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f'{collection_id}/mapped_with_content/'
        )
        page_list = [obj['Key'].split('/')[-1] for obj in response['Contents']]

    return page_list


def create_new_index(collection_id):
    page_list = get_page_list(collection_id)

    datetime_string = datetime.today().strftime('%Y%m%d%H%M%S')
    index_name = f"rikolti-{collection_id}-{datetime_string}"

    for page in page_list:
        add_page(page, collection_id, index_name)

    update_alias_for_collection_index("rikolti-stg", collection_id, index_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add collection data to OpenSearch")
    parser.add_argument('collection_id', help='Registry collection ID')
    args = parser.parse_args(sys.argv[1:])
    create_new_index(args.collection_id)
    sys.exit(0)
