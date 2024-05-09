import json
import requests

from .add_page_to_index import add_page
from . import settings
from .utils import print_opensearch_error

def update_stage_index_for_collection(collection_id: str, version_pages: list[str]):
    ''' update stage index with a new set of collection records '''
    index = get_index_for_alias(settings.STAGE_ALIAS)

    # delete existing records
    delete_collection_records_from_index(collection_id, index)

    # add pages of records to index
    for version_page in version_pages:
        add_page(version_page, index)

def get_index_for_alias(alias: str):
    # for now, there should be only one index per alias (stage, prod)
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(url, auth=settings.get_auth())
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    aliased_indices = [key for key in r.json().keys()]
    if len(aliased_indices) != 1:
        raise ValueError(
            f"Alias `{alias}` has {len(aliased_indices)} aliased indices. There should be 1.")
    else:
        return aliased_indices[0]

def delete_collection_records_from_index(collection_id: str, index: str):
    url = f"{settings.ENDPOINT}/{index}/_delete_by_query"
    headers = {"Content-Type": "application/json"}

    data = {
        "query": {
            "term": {
                "collection_id": collection_id
            }
        }
    }

    r = requests.post(
        url=url, data=json.dumps(data), headers=headers, auth=settings.get_auth())
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(f"deleted records with collection_id `{collection_id}` from index `{index}`")
