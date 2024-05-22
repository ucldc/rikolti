import json

import requests

from .. import settings

from utils.registry_client import registry_endpoint

etl_collection_url = (
    "https://registry.cdlib.org/api/v1/rikolticollection/"
    "?harvest_type=etl&format=json"
)

class OpensearchClient(object):
    def __init__(self, endpoint, auth):
        self.endpoint = endpoint
        self.auth = auth
    
    def search(self, **kwargs):
        resp = requests.get(
            f"{settings.ENDPOINT}/_search", 
            headers={"Content-Type": "application/json"},
            auth=settings.get_auth(),
            data=json.dumps(kwargs)
        )
        return resp.json()
    
    def update_by_query(self, **kwargs):
        resp = requests.post(
            f"{settings.ENDPOINT}/rikolti-stg/_update_by_query",
            headers={"Content-Type": "application/json"},
            auth=settings.get_auth(),
            data=json.dumps(kwargs)
        )
        json_resp = resp.json()

        if json_resp['timed_out'] or json_resp['failures']:
            raise Exception(f"TIMEOUT ERROR: \n{json.dumps(kwargs)}")
        
        return json_resp


class FacetError(Exception):
    pass

class FacetValueError(Exception):
    pass

def check_sort_collection_data(response):
    facet = response['aggregations']['sort_collection_data']
    total = response['hits']['total']['value']

    if len(facet['buckets']) != 1:
        raise(FacetError(f"Please check facet values: {facet}"))

    value = facet['buckets'][0]['key']
    count = facet['buckets'][0]['doc_count']
    print(f"{count}/{total} objects with this sort_collection_data value")
    print(value)

    if count != total:
        raise(FacetError(f"Please check count discrepancies: {count}/{total}"))

    if "registry.cdlib.org" not in value:
        raise(FacetValueError(f"registry.cdlib.org not in \n{value}"))
    
    if len(value.split("::")) < 4:
        raise(FacetValueError(f"4 or more parts required in \n{value}"))

opensearch = OpensearchClient(settings.ENDPOINT, settings.get_auth())

skipped_collections = []
ignored_collections = []
good_collections = []

for collection in registry_endpoint(etl_collection_url):
    collection_id = collection['id']
    print(f"Getting sort_collection_data for collection: {collection_id}")

    response = opensearch.search(
        query = {"terms": {"collection_url": [collection_id]}},
        aggs = {"sort_collection_data": {
            "terms": {
                "field": "sort_collection_data", 
                "size": 10000, 
                "order": {"_key": "asc"}
            }
        }}
    )

    try:
        check_sort_collection_data(response)
    except FacetError as e:
        print(e)
        action = input("Please type 'save' to save this error for later, or "
                       "press enter to continue...")
        if action == 'save':
            skipped_collections.append((collection, e))
        else:
            ignored_collections.append((collection, e))
        continue
    except FacetValueError as e:
        print(e)
        action = input("Please type 'save' to save this error for later, or "
                       "press enter to continue if the sort_collection_value "
                       "is already in 3 parts without a registry url")
        if action == 'save':
            skipped_collections.append((collection, e))
        else:
            good_collections.append((collection, e))
        continue

    sort_collection_data_value = (
        response['aggregations']
                ['sort_collection_data']
                ['buckets'][0]
                ['key']
    )
    doc_count = (
        response['aggregations']
                ['sort_collection_data']
                ['buckets'][0]
                ['doc_count']
    )

    sort_collection_data_parts = sort_collection_data_value.split("::")
    sortable_name = sort_collection_data_parts[0]
    display_name = ':'.join(sort_collection_data_parts[1:-2])
    new_value = "::".join([sortable_name, display_name, str(collection_id)])
    print(new_value)

    if len(sort_collection_data_parts) > 4:
        double_check = input("If the new value above looks good, please type "
                             "'yes' to continue...")
        if double_check != "yes":
            skipped_collections.append((collection, new_value))


    new_value = [new_value]

    update_response = opensearch.update_by_query(
        query={"terms": {"collection_url": [str(collection_id)]}},
        script={
            "source": "ctx._source.sort_collection_data = params.data",
            "lang": "painless",
            "params": {
                "data": new_value
            }
        }
    )

    if update_response['total'] != update_response['updated'] != doc_count:
        raise ValueError(update_response)

    print(update_response)

    input("press enter to continue...")

print('---------------')
print(skipped_collections)
print('---------------')
print(ignored_collections)
print('---------------')
print(good_collections)
print('---------------')