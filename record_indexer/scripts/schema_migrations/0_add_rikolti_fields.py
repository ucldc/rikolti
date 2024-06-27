import time
import sys
import json

from datetime import datetime
from urllib.parse import urlparse

from opensearchpy import OpenSearch, RequestsHttpConnection
from ... import settings

def get_client():
    if not settings.ENDPOINT:
        raise ValueError("Missing OPENSEARCH_ENDPOINT in environment")

    host_url = urlparse(settings.ENDPOINT)

    client = OpenSearch(
        hosts=[{
            'host': host_url.hostname,
            'port': host_url.port or 443,
        }],
        http_auth=settings.get_auth(),
        use_ssl=True,
        verify_certs=settings.verify_certs(),
        ssl_show_warn=settings.verify_certs(),
        connection_class=RequestsHttpConnection,
    )
    return client


def main():
    alias = "rikolti-stg"
    os_client = get_client()

    # get name of index currently aliased to rikolti-stg
    source_index = os_client.indices.get_alias(alias).keys()
    if len(source_index) > 1:
        raise ValueError(f"Alias `{alias}` has {len(source_index)} aliased "
                         "indices. There should be 1.")
    source_index = list(source_index)[0]

    # create new index name
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    destination_index = f"{alias}-{version}"

    # create migration script
    indexed_at = datetime.now().isoformat()

    field = "rikolti"
    initial_value = {"rikolti_value": {
        "version_path": "initial",
        "indexed_at": indexed_at,
        "page": "unknown"
    }}
    script = {
        "source": f"ctx._source.{field} = params.rikolti_value",
        "params": initial_value,
        "lang": "painless"
    }

    # reindex data from source to destination index
    task = os_client.reindex(
        body={
            "source": {"index": source_index},
            "dest": {"index": destination_index},
            "script": script
        },
        params={
            "wait_for_completion": "false",
            "error_trace": "true"
        }
    )
    task_id = task.get('task')
    print(f"{task_id=}")

    # poll task API until reindexing is complete
    task_state = os_client.tasks.get(task_id)
    while not task_state.get('completed'):
        time.sleep(5)
        task_state = os_client.tasks.get(task_id)
    
    print("Reindexing complete")
    if "error" not in task_state:
        print("Reindexing successful")
        removal = os_client.indices.delete_alias(source_index, alias)
        print(f"{removal=}")
        addition = os_client.indices.put_alias(destination_index, alias)
        print(f"{addition=}")
        print(
            f"'{alias}' alias removed from {source_index} and "
            f"added to {destination_index}"
        )
        print(
            f"Please verify {destination_index} looks good and "
            f"then delete {source_index}"
        )
    else:
        print("Reindexing failed")
        print(json.dumps(task_state))

if __name__ == "__main__":
    main()
    sys.exit()