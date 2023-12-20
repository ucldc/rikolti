import json

from pprint import pprint

import requests

from . import settings
from rikolti.utils.versions import (
    get_merged_page_content, get_with_content_urls_page_content)


def bulk_add(records: list, index: str):
    data = build_bulk_request_body(records, index)
    url = f"{settings.ENDPOINT}/_bulk"

    headers = {"Content-Type": "application/json"}

    r = requests.post(url, headers=headers, data=data, auth=settings.AUTH)
    r.raise_for_status()

    bulk_resp = r.json()
    if bulk_resp.get('errors') is True:
        error_reasons = []
        errors = []
        for bulk_item in bulk_resp.get('items'):
            for action, action_resp in bulk_item.items():
                if 'error' in action_resp:
                    error_reasons.append(action_resp['error'].get('reason'))
                    errors.append(bulk_item)

        error_reasons = list(set(error_reasons))
        if len(error_reasons) > 1:
            pprint(errors)
        raise(
            Exception(
                f"{len(errors)} errors in bulk indexing "
                f"{len(records)} records: {error_reasons}"
            )
        )


def build_bulk_request_body(records: list, index: str):
    # https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/
    body = ""
    for record in records:
        doc_id = record.get("calisphere-id")

        action = {"create": {"_index": index, "_id": doc_id}}

        body += f"{json.dumps(action)}\n{json.dumps(record)}\n"

    return body


def remove_unexpected_fields(record: dict, expected_fields: list):
    removed_fields = []
    for field in list(record.keys()):
        if field not in expected_fields:
            removed_fields.append(field)
            record.pop(field)

    return removed_fields


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
    flag_removed_fields = {}
    for record in records:
        removed_fields = remove_unexpected_fields(record, expected_fields)
        calisphere_id = record.get("calisphere-id", None)
        for field in removed_fields:
            if field in flag_removed_fields:
                flag_removed_fields[field].append(calisphere_id)
            else:
                flag_removed_fields[field] = [calisphere_id]

    bulk_add(records, index)

    print(
        f"added {len(records)} records to index `{index}` from "
        f"page `{version_page}`"
    )
    for field, calisphere_ids in flag_removed_fields.items():
        if len(calisphere_ids) != len(records):
            print(
                f"    {len(calisphere_ids)} items had {field} "
                f"removed: `{calisphere_ids}`"
            )
        else:
            print(f"    all {len(records)} records had {field} field removed")

