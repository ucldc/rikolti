import json
import os
import requests

import boto3

from . import settings


def bulk_add(json_data: str, index: str):

    url = f"{settings.ENDPOINT}/_bulk"

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(
        url, headers=headers, data=json_data, auth=settings.AUTH)
    r.raise_for_status()
    print(f"bulk added data to {index}")


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


def index_records(page: str, collection_id: str):
    records = get_json_content(collection_id, page)

    # FIXME
    for record in records:
        record.pop('is_shown_at')

    index_name = f"rikolti-stg-{collection_id}"
    bulk_request_body = build_bulk_request_body(records, index_name)
    bulk_add(bulk_request_body, index_name)
