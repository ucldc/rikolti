from datetime import datetime
import sys

import json
import requests

from ..utils import print_opensearch_error
from .. import settings

def main():
    '''
    This script:
    - reindexes all records in the index currently aliased to `rikolti-stg`
    - removes the old index from the `rikolti-stg` alias
    - adds the new index to the `rikolti-stg` alias

    NOTE: see TODO below re needing to use the task API to avoid a 504 timeout error
    
    Make sure that the rikolti index template is up to date with the settings
    and mappings you want the new index to have before running this script:
    
    https://github.com/ucldc/rikolti/blob/main/record_indexer/README.md#create-opensearch-index-template
    '''

    alias = "rikolti-stg"
    headers = {"Content-Type": "application/json"}
    auth = settings.get_auth()

    # get name of index currently aliased to rikolti-stg
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(url, auth=settings.get_auth())
    r.raise_for_status()
    aliased_indices = [key for key in r.json().keys()]
    if len(aliased_indices) != 1:
        raise ValueError(
            f"Alias `{alias}` has {len(aliased_indices)} aliased indices. There should be 1.")
    else:
        source_index = aliased_indices[0]

    # get new index name
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    destination_index = f"rikolti-stg-{version}"

    # reindex
    url = f"{settings.ENDPOINT}/_reindex"
    data = {
        "source":{"index": source_index},
        "dest":{"index": destination_index}
    }
    print(f"Reindexing `{source_index}` into `{destination_index}`")
    r = requests.post(
            url,
            headers=headers,
            auth=auth,
            data=json.dumps(data)
        )
    # this results in a 504 Gateway timeout, but the reindex operation is still running
    # TODO: need to use the task API to poll and see when the operation has finished
    # https://opensearch.org/docs/2.11/api-reference/tasks/
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(r.json())

    # remove old index from rikolti-stg alias
    url = f"{settings.ENDPOINT}/_aliases"
    data = {
        "actions": [
            {"remove": {
                "indices": source_index, 
                "alias": alias
            }}
        ]
    }
    r = requests.post(
            url, 
            headers=headers, 
            data=json.dumps(data), 
            auth=auth
        )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(r.json())
    print(f"removed `{source_index}` from alias `{alias}`")

    # add new index to rikolti-stg alias
    url = f"{settings.ENDPOINT}/_aliases"
    data = {"actions": [{"add": {"index": index, "alias": alias}}]}
    r = requests.post(
        url, 
        headers=headers,
        data=json.dumps(data), 
        auth=auth
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(f"added index `{index}` to alias `{alias}`")

if __name__ == "__main__":
    main()
    sys.exit()