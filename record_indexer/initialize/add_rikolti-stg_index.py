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


def main(name=None):
    # check if index with rikolti-stg alias exists
    url = f"{settings.ENDPOINT}/_alias/rikolti-stg"
    r = requests.get(
        url,
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if r.status_code == 200:
        print("Index with alias rikolti-stg already exists; OpenSearch "
              "instance ready for use")
        return
    
    # create index
    if not name:
        name = datetime.now().strftime("%Y%m%d%H%M%S")

    index_name = f"rikolti-{name}"
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
    print(r.text)

    # add alias
    url = f"{settings.ENDPOINT}/_aliases"
    data = {
        "actions": [
            {"add": {"index": index_name, "alias": "rikolti-stg"}}
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
    print(r.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Creates an empty index at rikolti-<name>; if no name "
            "provided, uses the current timestamp. Adds the index to "
            "the rikolti-stg alias."
        )
    )
    parser.add_argument(
        "-n", "--name", 
        help=(
            "Optional name for the index; created index will be "
            "rikolti-<name>, if none provided, current timestamp will be used"
        )
    )
    args = parser.parse_args()
    sys.exit(main(args.name))
