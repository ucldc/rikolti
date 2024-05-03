import os
import sys
import traceback

import json
from pprint import pprint
import requests

def print_opensearch_error(r: requests.Response, url: str):
    direct_frame = traceback.extract_stack(limit=2)[0]
    print(f"ERROR @ {direct_frame.name} from {direct_frame.filename}")
    try:
        pprint(r.json())
    except requests.exceptions.JSONDecodeError:
        print(f"{r.status_code}: {url}")
        print(f"Response: {r.text}")

def main():
    source_endpoint = os.environ.get("SOURCE_ENDPOINT")
    target_endpoint = os.environ.get("TARGET_ENDPOINT")
    headers = {"Content-Type": "application/json"}
    auth = (os.environ.get("OPENSEARCH_USER"), os.environ.get("OPENSEARCH_PASS"))

    # Get list of indices to migrate
    url = f"{source_endpoint}/_alias/rikolti-stg"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    indices_to_migrate = [key for key in r.json().keys()]
    
    for index in indices_to_migrate:
        # Copy index from source to target domain
        data = {
            "source": {
                "remote": {
                    "host": f"{source_endpoint}:443",
                    "username": os.environ.get("OPENSEARCH_USER"),
                    "password": os.environ.get("OPENSEARCH_PASS")
                },
                "index": index
            },
            "dest": {
                "index": index
            }
        }
        url = f"{target_endpoint}/_reindex"
        
        r = requests.post(url, headers=headers, data=json.dumps(data), auth=auth)
        print(f"{r.json()}\n")
        r.raise_for_status()

        # Add new index to rikolti-stg alias
        url = f"{target_endpoint}/_aliases"
        data = {"actions": [{"add": {"index": index, "alias": "rikolti-stg"}}]}

        r = requests.post(
            url, headers=headers, data=json.dumps(data), auth=auth)
        if not (200 <= r.status_code <= 299):
            print_opensearch_error(r, url)
            r.raise_for_status()
        print(f"added index `{index}` to rikolti-stg alias")

if __name__ == "__main__":
    main()
    sys.exit()