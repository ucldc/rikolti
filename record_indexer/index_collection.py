import json
import requests
from datetime import datetime

from .add_page_to_index import add_page
from . import settings
from .utils import print_opensearch_error
from rikolti.utils.versions import get_version


def index_collection(alias: str, collection_id: str, version_pages: list[str]):
    '''
    find 1 index at alias and update it with records from version_pages
    '''
    index = get_index_for_alias(alias)

    version_path = get_version(collection_id, version_pages[0])
    rikolti_data = {
        "version_path": version_path,
        "indexed_at": datetime.now().isoformat(),
    }

    # add pages of records to index
    for version_page in version_pages:
        add_page(version_page, index, rikolti_data)

    # delete existing records
    delete_collection_records_from_index(collection_id, index, version_path)


def get_index_for_alias(alias: str):
    # for now, there should be only one index per alias (stage, prod)
    url = f"{settings.ENDPOINT}/_alias/{alias}"
    r = requests.get(
        url,
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    aliased_indices = [key for key in r.json().keys()]
    if len(aliased_indices) != 1:
        raise ValueError(
            f"Alias `{alias}` has {len(aliased_indices)} aliased indices. There should be 1.")
    else:
        return aliased_indices[0]


def delete_collection_records_from_index(
        collection_id: str, index: str, version_path: str):
    """
    Delete records from index that have the same collection_id but an outdated
    version_path
    """
    url = f"{settings.ENDPOINT}/{index}/_delete_by_query"
    headers = {"Content-Type": "application/json"}

    data = {
        "query": {
            "bool": {
                "must": {"term": {"collection_id": collection_id}},
                "must_not": {"term": {"rikolti.version_path": version_path}},
            }
        }
    }

    r = requests.post(
        url=url,
        data=json.dumps(data),
        headers=headers,
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(f"deleted records with collection_id `{collection_id}` from index `{index}`")
