import argparse
import json
import sys

import requests

from ..utils import print_opensearch_error
from .. import settings


def delete_collection(collection_id, alias):
    # get name of index currently aliased to alias
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(url, auth=settings.get_auth())
    r.raise_for_status()
    aliased_indices = [key for key in r.json().keys()]
    if len(aliased_indices) != 1:
        raise ValueError(
            f"Alias `{alias}` has {len(aliased_indices)} aliased indices. There should be 1.")
    else:
        index = aliased_indices[0]

    # delete collection records from index
    data = {
        "query": {
            "term": {
                "collection_url": {
                    "value": collection_id
                }
            }
        }
    }

    r = requests.post(
        f"{settings.ENDPOINT}/{index}/_delete_by_query",
        headers = {"Content-Type": "application/json"},
        data = json.dumps(data),
        auth = settings.get_auth()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(r.json())   


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("collection_id", help="Registry ID of collection to delete")
    args = parser.parse_args()
    delete_collection(args.collection_id, "rikolti-stg")

    sys.exit()