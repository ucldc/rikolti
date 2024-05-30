from datetime import datetime
import sys

import time

from .. import settings
from .reindex_rikolti_stg import OpensearchClient

def main():

    alias = "rikolti-stg"
    os_client = OpensearchClient(settings.ENDPOINT, settings.get_auth())

    # get name of index currently aliased to rikolti-stg
    source_index = os_client.get_aliased_indexes(alias)[0]

    # create new index name
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    destination_index = f"rikolti-prod-{version}"

    # reindex
    task_id = os_client.reindex(source_index, destination_index)

    # poll task API until reindexing is complete
    task_state = os_client.get_task(task_id)
    while not task_state.get('completed'):
        time.sleep(5)
        task_state = os_client.get_task(task_id)
    
    print("Reindexing from {source_index} to {destination_index} complete")

    add_json_resp = os_client.add_alias(destination_index, "rikolti-prd")
    print(add_json_resp)

if __name__ == "__main__":
    main()
    sys.exit()