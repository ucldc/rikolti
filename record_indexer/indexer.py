import json
import os

import boto3
import requests

from . import settings


def bulk_add(json_data: str, index: str):

    url = f"{settings.ENDPOINT}/_bulk"

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(
        url, headers=headers, data=json_data, auth=settings.AUTH)
    r.raise_for_status()


def build_bulk_request_body(records: list, index: str):
    # https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/
    body = ""
    for record in records:
        doc_id = record.get("calisphere-id")

        action = {
            "create": {
                "_index": index,
                "_id": doc_id
            }
        }

        body += f"{json.dumps(action)}\n{json.dumps(record)}\n"

    return body


def get_json_content(collection_id: str, filename: str):
    if settings.DATA_SRC["STORE"] == 'file':
        local_path = settings.local_path(
            'mapped_with_content', collection_id)
        path = os.path.join(local_path, str(filename))
        file = open(path, "r")
        records = json.loads(file.read())
    else:
        s3_client = boto3.client('s3')
        file = s3_client.get_object(
            Bucket=settings.DATA_SRC["BUCKET"],
            Key=f"mapped_with_content/{collection_id}/{filename}"
        )
        records = json.loads(file['Body'].read())

    return records

def add_index_to_alias(index: str, alias: str, collection_id: str):
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
    json_data = json.dumps(data)

    r = requests.post(
        url, headers=headers, data=json_data, auth=settings.AUTH)
    r.raise_for_status()
    print(f"added index `{index}` to alias `{alias}`")


def remove_indices_from_alias(collection_id: str, alias: str):
    url = f"{settings.ENDPOINT}/rikolti-{collection_id}-*"
    r = requests.head(url=url, auth=settings.AUTH, params={'allow_no_indices':'false'})
    if r.status_code == 404:
        return
    else:
        url = f"{settings.ENDPOINT}/_aliases"
        headers = {
            "Content-Type": "application/json"
        }

        indices = f"rikolti-{collection_id}-*"
        data = {
            "actions": [
                {
                    "remove": {
                        "indices": [indices],
                        "alias": alias
                    }
                }
            ]
        }
        json_data = json.dumps(data)

        r = requests.post(
            url, headers=headers, data=json_data, auth=settings.AUTH)
        r.raise_for_status()
        print(f"removed indices `{indices}` from alias `{alias}`")


def add_page(page: str, collection_id: str, index: str):
    records = get_json_content(collection_id, page)

    # FIXME Create an alert for unexpected fields and remove from json
    for record in records:
        record.pop('is_shown_at', None)

    bulk_request_body = build_bulk_request_body(records, index)
    bulk_add(bulk_request_body, index)

    print(f"added page `{page}` to index `{index}`")


def delete_index(index: str):
    url = f"{settings.ENDPOINT}/{index}"

    r = requests.delete(url, auth=settings.AUTH)
    r.raise_for_status()
    print(f"deleted index {index}")
