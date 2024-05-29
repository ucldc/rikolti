from datetime import datetime
import sys

import json
import requests
import time

from ..utils import print_opensearch_error
from .. import settings

class OpensearchClient(object):
    def __init__(self, endpoint, auth):
        self.endpoint = endpoint
        self.auth = auth

    def get_aliased_indexes(self, alias):
        '''
        get the names of all indexes currently aliased to `alias`
        '''
        url = f"{self.endpoint}/_alias/{alias}"
        resp = requests.get(url, auth=self.auth)
        resp.raise_for_status()
        aliased_indices = [key for key in resp.json().keys()]
        if len(aliased_indices) != 1:
            raise ValueError(
                f"Alias `{alias}` has {len(aliased_indices)} aliased indices. "
                "There should be 1."
            )
        else:
            return aliased_indices


    def reindex(self, source_index, destination_index):
        '''
        reindex all records from `source_index` into `destination_index`
        '''
        url = f"{self.endpoint}/_reindex"
        data = {
            "source": {"index": source_index},
            "dest": {"index": destination_index}
        }
        print(f"Reindexing `{source_index}` into `{destination_index}`")
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            auth=self.auth,
            params={"wait_for_completion": "false"},
            data=json.dumps(data)
        )
        if not (200 <= resp.status_code <= 299):
            print_opensearch_error(resp, f"{self.endpoint}/_reindex")
            resp.raise_for_status()
        return resp.json()['task']


    def remove_alias(self, index, alias):
        '''
        remove `index` from `alias`
        '''
        url = f"{self.endpoint}/_aliases"
        data = {
            "actions": [
                {"remove": {
                    "indices": index, 
                    "alias": alias
                }}
            ]
        }
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            auth=self.auth,
            data=json.dumps(data)
        )
        if not (200 <= resp.status_code <= 299):
            print_opensearch_error(resp, url)
            resp.raise_for_status()
        
        print(resp.json())
        print(f"removed `{index}` from alias `{alias}`")
        return resp.json()


    def add_alias(self, index, alias):
        '''
        add `index` to `alias`
        '''
        url = f"{self.endpoint}/_aliases"
        data = {
            "actions": [
                {"add": {
                    "index": index, 
                    "alias": alias
                }}
            ]
        }
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            auth=self.auth,
            data=json.dumps(data)
        )
        if not (200 <= resp.status_code <= 299):
            print_opensearch_error(resp, url)
            resp.raise_for_status()

        print(f"added index `{index}` to alias `{alias}`")
        return resp.json()


    def get_task(self, task_id):
        '''
        poll the task API until the task with `task_id` is complete
        '''
        url = f"{self.endpoint}/_tasks/{task_id}"
        resp = requests.get(url, auth=self.auth)
        if not (200 <= resp.status_code <= 299):
            print_opensearch_error(resp, url)
            resp.raise_for_status()

        return resp.json()


def main():
    '''
    This script:
    - reindexes all records in the index currently aliased to 
      `rikolti-stg`
    - removes the old index from the `rikolti-stg` alias
    - adds the new index to the `rikolti-stg` alias

    NOTE: see TODO below re needing to use the task API to avoid a 504 
    timeout error
    
    Make sure that the rikolti index template is up to date with the 
    settings and mappings you want the new index to have before running 
    this script:
    
    https://github.com/ucldc/rikolti/blob/main/record_indexer/README.md#create-opensearch-index-template
    '''

    alias = "rikolti-stg"
    os_client = OpensearchClient(settings.ENDPOINT, settings.get_auth())

    # get name of index currently aliased to rikolti-stg
    source_index = os_client.get_aliased_indexes(alias)[0]

    # create new index name
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    destination_index = f"rikolti-stg-{version}"

    # reindex
    task_id = os_client.reindex(source_index, destination_index)

    # poll task API until reindexing is complete
    task_state = os_client.get_task(task_id)
    while not task_state.get('completed'):
        time.sleep(5)
        task_state = os_client.get_task(task_id)
    
    print("Reindexing complete")

    removal_json_resp = os_client.remove_alias(source_index, alias)
    add_json_resp = os_client.add_alias(destination_index, alias)
    print(removal_json_resp)
    print(add_json_resp)

if __name__ == "__main__":
    main()
    sys.exit()