import json

import requests

from . import settings
from rikolti.utils.versions import (
    get_merged_page_content, get_with_content_urls_page_content)


def bulk_add(records: list, index: str):
    data = build_bulk_request_body(records, index)
    url = f"{settings.ENDPOINT}/_bulk"

    headers = {"Content-Type": "application/json"}

    r = requests.post(url, headers=headers, data=data, auth=settings.AUTH)

    if r.status_code != 200:
        raise requests.HTTPError(r.status_code, r.json())


def build_bulk_request_body(records: list, index: str):
    # https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/
    body = ""
    for record in records:
        doc_id = record.get("calisphere-id")

        action = {"create": {"_index": index, "_id": doc_id}}

        body += f"{json.dumps(action)}\n{json.dumps(record)}\n"

    return body


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
    record_schema = record_index_config["template"]["mappings"]["properties"]
    expected_fields = list(record_schema.keys())

    return expected_fields


def add_page(version_page: str, index: str):
    if 'merged' in version_page:
        records = get_merged_page_content(version_page)
    else:
        records = get_with_content_urls_page_content(version_page)

    expected_fields = get_expected_fields()
    for record in records:
        record = flag_and_remove_unexpected_fields(record, expected_fields)

    bulk_add(records, index)

    print(f"added page `{page}` to index `{index}`")
