import json
import requests
from datetime import datetime
from typing import Any

from .index_page import index_page
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
        # index page of records - the index action creates a document if
        # it doesn't exist, and replaces the document if it does
        index_page(version_page, index, rikolti_data)

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


def get_outdated_versions(index:str, query: dict[str, Any]):
    url = f"{settings.ENDPOINT}/{index}/_search"
    headers = {"Content-Type": "application/json"}

    data = dict(query, **{
        "aggs": {
            "version_paths": {
                "terms": {
                    "field": "rikolti.version_path",
                    "size": 10
                }
            }
        },
        "track_total_hits": True,
        "size": 0
    })

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

    return r.json()


def delete_collection_records_from_index(
        collection_id: str, index: str, version_path: str):
    """
    Delete records from index that have the same collection_id but an outdated
    version_path
    """
    data = {
        "query": {
            "bool": {
                "must": {"term": {"collection_id": collection_id}},
                "must_not": {"term": {"rikolti.version_path": version_path}},
            }
        }
    }

    outdated = get_outdated_versions(index, data)
    num_outdated_records = outdated.get(
        'hits', {}).get('total', {}).get('value', 0)
    oudated_versions = outdated.get(
        'aggregations', {}).get('version_paths', {}).get('buckets')

    if num_outdated_records > 0:
        url = f"{settings.ENDPOINT}/{index}/_delete_by_query"
        r = requests.post(
            url=url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            auth=settings.get_auth(),
            verify=settings.verify_certs()
        )
        if not (200 <= r.status_code <= 299):
            print_opensearch_error(r, url)
            r.raise_for_status()
        print(f"deleted records with collection_id `{collection_id}` from index `{index}`")
    else:
        print(f"No outdated records found for collection {collection_id} in `{index}` index.")

    return
