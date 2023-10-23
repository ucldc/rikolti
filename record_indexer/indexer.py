import json
import os

import boto3
import requests

from . import settings


def bulk_add(records: list, index:str):
    data = build_bulk_request_body(records, index)
    url = f"{settings.ENDPOINT}/_bulk"

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(
        url, headers=headers, data=data, auth=settings.AUTH)
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
            collection_id, 'mapped_with_content')
        path = os.path.join(local_path, str(filename))
        file = open(path, "r")
        records = json.loads(file.read())
    else:
        s3_client = boto3.client('s3')
        file = s3_client.get_object(
            Bucket=settings.DATA_SRC["BUCKET"],
            Key=f"{collection_id}/mapped_with_content/{filename}"
        )
        records = json.loads(file['Body'].read())

    return records


def flag_and_remove_unexpected_fields(record: dict, expected_fields: list):
    calisphere_id = record.get("calisphere-id", None)
    for field in list(record.keys()):
        if field not in expected_fields:
            print(f"unexpected field `{field}` found in record `{calisphere_id}`")
            print("   removing field from record")
            record.pop(field)

    return record


def get_expected_fields():
    record_index_config = json.load(open(settings.RECORD_INDEX_CONFIG))
    record_schema = record_index_config['template']['mappings']['properties']
    expected_fields = list(record_schema.keys())

    return expected_fields


def add_page(page: str, collection_id: str, index: str):
    records = get_json_content(collection_id, page)

    expected_fields = get_expected_fields()
    for record in records:
        record = flag_and_remove_unexpected_fields(record, expected_fields)

    bulk_add(records, index)

    print(f"added page `{page}` to index `{index}`")


def delete_index(index: str):
    url = f"{settings.ENDPOINT}/{index}"

    r = requests.delete(url, auth=settings.AUTH)
    r.raise_for_status()
    print(f"deleted index {index}")
