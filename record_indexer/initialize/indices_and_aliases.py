import argparse
import json
import sys
from datetime import datetime

import requests

from .. import settings
from ..utils import print_opensearch_error

"""
    Create a rikolti index and add the rikolti-stg alias to it
    https://opensearch.org/docs/2.3/opensearch/index-templates/
"""

def get_index_at_alias(alias):
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(
        url,
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if r.status_code != 200:
        print_opensearch_error(r, url)
        r.raise_for_status()

    if len(r.json().keys()) != 1:
        raise ValueError(f"Multiple indices found at alias {alias}")

    return r.json()


def create_index(index_name):
    url = f"{settings.ENDPOINT}/{index_name}"
    r = requests.put(
        url,
        headers={"Content-Type": "application/json"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    return r.json()


def add_alias(index_name, alias):
    url = f"{settings.ENDPOINT}/_aliases"
    data = {
        "actions": [
            {"add": {"index": index_name, "alias": alias}}
        ]
    }
    r = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(data),
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    return r.json()


def main(stg_name, prd_name):
    try:
        index = get_index_at_alias("rikolti-stg")
        print(f"Index `{index}` with alias rikolti-stg already exists, "
              "skipping creation")
    except requests.HTTPError:
        index_name = f"rikolti-{stg_name}"
        resp = create_index(index_name)
        print(resp)
        resp = add_alias(index_name, "rikolti-stg")
        print(resp)
    
    try:
        index = get_index_at_alias("rikolti-prd")
        print(f"Index `{index}` with alias rikolti-prd already exists, "
              "skipping creation")
    except requests.HTTPError:
        index_name = f"rikolti-{prd_name}"
        resp = create_index(index_name)
        print(resp)
        resp = add_alias(index_name, "rikolti-prd")
        print(resp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Creates an empty index at rikolti-<name>; if no name "
            "provided, uses the current timestamp. Adds the index to "
            "the rikolti-stg alias."
        )
    )
    parser.add_argument(
        "-s", "--stg-name", 
        help=(
            "Optional name for the rikolti-stg index; created index will be "
            "rikolti-<name>, if none provided, current timestamp will be used"
        ),
        default=datetime.now().strftime("%Y%m%d%H%M%S")
    )
    parser.add_argument(
        "-p", "--prd-name", 
        help=(
            "Optional name for the rikolti-prd index; created index will be "
            "rikolti-<name>, if none provided, current timestamp will be used"
        ),
        default=datetime.now().strftime("%Y%m%d%H%M%S")
    )
    args = parser.parse_args()
    sys.exit(main(args.stg_name, args.prd_name))
