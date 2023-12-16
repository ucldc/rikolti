import argparse
from datetime import datetime
import json
import os
import sys

import boto3
import requests

from .add_page_to_index import add_page
from . import settings
from rikolti.utils.versions import get_merged_pages, get_with_content_urls_pages


def update_alias_for_collection(alias: str, collection_id: str, index: str):
    remove_collection_indices_from_alias(alias, collection_id)

    url = f"{settings.ENDPOINT}/_aliases"
    headers = {"Content-Type": "application/json"}

    data = {"actions": [{"add": {"index": index, "alias": alias}}]}

    r = requests.post(url, headers=headers, data=json.dumps(data), auth=settings.AUTH)
    r.raise_for_status()
    print(f"added index `{index}` to alias `{alias}`")


def remove_collection_indices_from_alias(alias: str, collection_id: str):
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(url=url, auth=settings.AUTH)
    if r.status_code == 404:
        return
    else:
        r.raise_for_status()
        indices = json.loads(r.text)
        indices_to_remove = [
            key for key in indices if key.startswith(f"rikolti-{collection_id}-")
        ]

        if len(indices_to_remove) > 0:
            url = f"{settings.ENDPOINT}/_aliases"
            headers = {"Content-Type": "application/json"}
            data = {
                "actions": [{"remove": {"indices": indices_to_remove, "alias": alias}}]
            }

            r = requests.post(
                url, headers=headers, data=json.dumps(data), auth=settings.AUTH
            )
            r.raise_for_status()
            print(f"removed indices `{indices_to_remove}` from alias `{alias}`")


def delete_old_collection_indices(collection_id: str):
    """
    Deletes older unaliased indices, retaining a specified number
    """
    url = f"{settings.ENDPOINT}/rikolti-{collection_id}-*"
    params = {"ignore_unavailable": "true"}
    r = requests.get(url=url, params=params, auth=settings.AUTH)
    r.raise_for_status()
    indices = json.loads(r.text)

    unaliased_indices = {}
    for index in indices.keys():
        if not indices[index]["aliases"]:
            creation_date = indices[index]["settings"]["index"]["creation_date"]
            unaliased_indices[creation_date] = index

    counter = 0
    for date in reversed(sorted(unaliased_indices)):
        counter += 1
        if counter > int(settings.INDEX_RETENTION):
            delete_index(unaliased_indices[date])


def delete_index(index: str):
    url = f"{settings.ENDPOINT}/{index}"

    r = requests.delete(url, auth=settings.AUTH)
    if r.status_code == 404:
        return
    else:
        r.raise_for_status()
        print(f"deleted index `{index}`")


def create_new_index(collection_id: str, version_pages: list[str]):
    # Once we start keeping dated versions of mapped metadata on S3,
    # the version will correspond to the S3 namespace
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    index_name = f"rikolti-{collection_id}-{version}"

    # OpenSearch creates the index on the fly when first written to.
    for version_page in version_pages:
        add_page(version_page, index_name)

    update_alias_for_collection("rikolti-stg", collection_id, index_name)

    delete_old_collection_indices(collection_id)

    return index_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add collection data to OpenSearch")
    parser.add_argument("collection_id", help="Registry collection ID")
    parser.add_argument("version", help="Metadata verison to index")
    args = parser.parse_args(sys.argv[1:])
    if 'merged' in args.version:
        page_list = get_merged_pages(args.version)
    else:
        page_list = get_with_content_urls_pages(args.version)
    create_new_index(args.collection_id, page_list)
    sys.exit(0)
