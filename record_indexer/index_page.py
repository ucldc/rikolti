import json
from collections import defaultdict

from pprint import pprint

import requests

from . import settings
from .utils import print_opensearch_error
from .index_templates.record_index_config import RECORD_INDEX_CONFIG
from rikolti.utils.versions import (
    get_merged_page_content, get_with_content_urls_page_content)


def bulk_add(records: list, index: str):
    data = build_bulk_request_body(records, index)
    url = f"{settings.ENDPOINT}/_bulk"

    headers = {"Content-Type": "application/json"}

    r = requests.post(
        url,
        headers=headers,
        data=data,
        params={"refresh": "true"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()

    bulk_resp = r.json()
    if bulk_resp.get('errors') is True:
        error_reasons = []
        errors = []
        for bulk_item in bulk_resp.get('items'):
            for action, action_resp in bulk_item.items():
                if 'error' in action_resp:
                    if action_resp['error'].get('type') == 'version_conflict_engine_exception':
                        print(f"WARNING - document already exists; not creating.\n {bulk_item}")
                    else:
                        error_reasons.append(action_resp['error'].get('reason'))
                        errors.append(bulk_item)

        error_reasons = list(set(error_reasons))
        if len(error_reasons) > 1:
            pprint(errors)
        if len(errors):
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
        doc_id = record.get("id")

        action = {"index": {"_index": index, "_id": doc_id}}

        body += f"{json.dumps(action)}\n{json.dumps(record)}\n"

    return body


def remove_unexpected_fields(record: dict, expected_fields: list):
    removed_fields = []
    for field in list(record.keys()):
        if field not in expected_fields:
            removed_fields.append(field)
            record.pop(field)

    # TODO: not sure if we want to qualify field names with the parent id
    parent_id = record.get('calisphere-id')
    for child in record.get('children', []):
        removed_fields_from_child = remove_unexpected_fields(
            child, expected_fields)
        removed_fields += [
            f"{parent_id}[{field}]" for field in removed_fields_from_child
        ]

    return removed_fields


def get_expected_fields():
    record_schema = RECORD_INDEX_CONFIG["template"]["mappings"]["properties"]
    expected_fields = list(record_schema.keys())

    return expected_fields


def index_page(version_page: str, index: str, rikolti_data: dict):
    if 'merged' in version_page:
        records = get_merged_page_content(version_page)
    else:
        records = get_with_content_urls_page_content(version_page)

    expected_fields = get_expected_fields()
    removed_fields_report = defaultdict(list)
    for record in records:
        removed_fields = remove_unexpected_fields(record, expected_fields)
        calisphere_id = record.get("calisphere-id", None)
        for field in removed_fields:
            removed_fields_report[field].append(calisphere_id)

        record['rikolti'] = dict(record.get('rikolti', {}), **rikolti_data)
        record['rikolti']['page'] = version_page.split('/')[-1]

    bulk_add(records, index)

    start = "\n" + "-"*40 + "\n"
    end = "\n" + "~"*40 + "\n"

    message = (
        f"{start}Indexed {len(records)} records to index `{index}` from "
        f"page `{version_page}`\n"
    )
    for field, calisphere_ids in removed_fields_report.items():
        if len(calisphere_ids) != len(records):
            message += (
                f"{' '*5}{len(calisphere_ids)} items had {field} "
                f"removed: `{calisphere_ids}`\n"
            )
        else:
            message += (
                f"{' '*5}all {len(records)} records had {field} field "
                "removed\n"
            )
    message += end
    print(message)

