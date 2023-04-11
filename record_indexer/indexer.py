import json
import requests
import settings


def bulk_add(records, index):

    bulk_request_body = build_bulk_request_body(records, index)

    url = f"{settings.ENDPOINT}/_bulk"

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(
        url, headers=headers, data=bulk_request_body, auth=settings.AUTH)
    r.raise_for_status()
    print(r.text)


def build_bulk_request_body(records, index):
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
