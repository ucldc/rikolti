import json
import requests

import boto3

from . import settings


def bulk_add(data: str, index: str):

    url = f"{settings.ENDPOINT}/_bulk"

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(
        url, headers=headers, data=data, auth=settings.AUTH)
    r.raise_for_status()
    print(r.text)


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
    if settings.DATA_SRC == 'local':
        local_path = settings.local_path(
            'mapped_with_content', collection_id)
        path = os.path.join(local_path, str(filename))
        file = open(path, "r")
        record = json.loads(file.read())
    else:
        s3_client = boto3.client('s3')
        file = s3_client.get_object(
            Bucket=settings.DATA_SRC["BUCKET"],
            Key=f"mapped_with_content/{collection_id}/{filename}"
        )
        record = json.loads(file['Body'].read())

    return record


def index_records(files: list, collection_id: str):
    # each file contains metadata for one record
    # aggregate records for bulk loading
    records = [get_json_content(collection_id, file) for file in files]
    print(f"{records=}")
    #index_name = "rikolti-test"
    #bulk_request_body = build_bulk_request_body(records, index)
    #bulk_add(records, index_name)
