import argparse
from datetime import datetime
import sys

import time

from .. import settings
from .reindex_rikolti_stg import OpensearchClient

def main(src, dest, **kwargs):
    os_client = OpensearchClient(settings.ENDPOINT, settings.get_auth())

    if kwargs.get("alias"):
        alias = src
        # get name of index currently aliased to rikolti-stg
        source_index = os_client.get_aliased_indexes(alias)[0]
    else:
        source_index = src

    # create new index name
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    destination_index = f"{dest}-{version}"

    print(f"Reindexing from {source_index} to {destination_index}")

    # reindex
    task_id = os_client.reindex(source_index, destination_index)

    # poll task API until reindexing is complete
    task_state = os_client.get_task(task_id)
    while not task_state.get('completed'):
        time.sleep(5)
        task_state = os_client.get_task(task_id)
    
    print(f"Reindexing from {source_index} to {destination_index} complete")

    if kwargs.get("dest_alias"):
        dest_alias = kwargs.get("dest_alias")
        print(f"Adding {destination_index} to alias {dest_alias}")
        add_json_resp = os_client.add_alias(destination_index, dest_alias)
        print(add_json_resp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.usage = (
        "python copy_index.py src dest [options]\n"
        "\t--alias add this flag if src is an alias\n"
        "\t--dest_alias add dest to an alias\n\n\n"
    )
    parser.description = (
        "This script will copy an index to a new index and optionally "
        "add the new index to an alias.\n\nThe source index to copy can "
        "be specified by either index name or alias name. If an alias "
        "source name is specified, the first index with that alias "
        "will be the one that's copied."
    )
    parser.add_argument("src", help=(
        "The source to copy from (can be either an index name or an "
        "alias, with the --alias flag)"
    ))
    parser.add_argument("dest", help="The destination to copy to")
    parser.add_argument("--alias", help="Copy from an alias", action="store_true")
    parser.add_argument("--dest_alias", help="The alias to add to the destination index")
    
    args = parser.parse_args()
    kwargs = vars(args)
    main(**kwargs)
    sys.exit()