import argparse
from datetime import datetime
import json
import os
import sys
from urllib.parse import urlparse

import boto3
import requests

from .indexer import add_page
from . import settings


def update_alias_for_collection(alias: str, collection_id: str, index: str):
    remove_collection_indices_from_alias(alias, collection_id)

    url = f"{settings.ENDPOINT}/_aliases"
    headers = {
        "Content-Type": "application/json"
    }

    data = {
        "actions": [
            {
                "add": {
                    "index": index,
                    "alias": alias
                }
            }
        ]
    }

    r = requests.post(
        url, headers=headers, data=json.dumps(data), auth=settings.AUTH)
    r.raise_for_status()
    print(f"added index `{index}` to alias `{alias}`")


def remove_collection_indices_from_alias(alias: str, collection_id: str):
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(url=url, auth=settings.AUTH)
    r.raise_for_status()
    aliases = json.loads(r.text)
    indices_to_remove = [key for key in aliases if key.startswith(f"rikolti-{collection_id}-")]

    if len(indices_to_remove) > 0:
        url = f"{settings.ENDPOINT}/_aliases"
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "actions": [
                {
                    "remove": {
                        "indices": indices_to_remove,
                        "alias": alias
                    }
                }
            ]
        }

        r = requests.post(
            url, headers=headers, data=json.dumps(data), auth=settings.AUTH)
        r.raise_for_status()
        print(f"removed indices `{indices_to_remove}` from alias `{alias}`")


def get_page_list(collection_id: str):
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


def create_new_index(collection_id: str):
    page_list = get_page_list(collection_id)

    datetime_string = datetime.today().strftime('%Y%m%d%H%M%S')
    index_name = f"rikolti-{collection_id}-{datetime_string}"

    for page in page_list:
        add_page(page, collection_id, index_name)

    update_alias_for_collection("rikolti-stg", collection_id, index_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add collection data to OpenSearch")
    parser.add_argument('collection_id', help='Registry collection ID')
    args = parser.parse_args(sys.argv[1:])
    create_new_index(args.collection_id)
    sys.exit(0)
