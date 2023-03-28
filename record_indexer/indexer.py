import os
import json
import requests

ENDPOINT = os.environ.get('RIKOLTI_ES_ENDPOINT')
AUTH = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

def bulk_add(records, index):

    bulk_request_body = build_bulk_request_body(records, index)

    url = os.path.join(ENDPOINT, "_bulk")

    headers = {
        "Content-Type": "application/json"
    }
    
    r = requests.post(url, headers=headers, data=bulk_request_body, auth=AUTH)
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

        document = record

        body += f"{json.dumps(action)}\n{json.dumps(record)}\n"

    return body
