from temporalio import activity
from temporalio import workflow
from temporalio.client import Client
import json

import sys, os
root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)
from metadata_fetcher.fetchers.oai_fetcher import OaiFetcher
from metadata_fetcher.fetchers.Fetcher import InvalidHarvestEndpoint

from scripts.map_page import map_page
import settings


@activity.defn
async def get_first_payload(collection: str) -> str:

    payload = {
        # 26774 OAI Palos Verdes Bulletin - 82 items
        "collection_id": 26774,
        "harvest_type": "oai",
        "harvest_data": {
          "url": "http://www.palosverdeshistory.org/oai2",
          "harvest_extra_data": "pvld_4048"
        }
    }

    return json.dumps(payload)

@activity.defn
async def fetch_metadata_page(payload: str) -> str:
    print(f"fetching metadata page")
    try:
        fetcher = OaiFetcher(json.loads(payload))
        fetcher.fetch_page()
    except InvalidHarvestEndpoint as e:
        print(e)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        }

    next_page = fetcher.json()

    return next_page

@activity.defn
async def get_vernacular_page_list(collection: str) -> list:
    print(f"Get list of Vernacular metadata files for {collection}")

    # quick sketch. Move this into metadata_mapper/ dir?
    vernacular_path = settings.local_path('vernacular_metadata', collection)
    page_list = [f for f in os.listdir(vernacular_path)
                 if os.path.isfile(os.path.join(vernacular_path, f))]
    children_path = os.path.join(vernacular_path, 'children')
    if os.path.exists(children_path):
        page_list += [os.path.join('children', f) for f in os.listdir(children_path)
                      if os.path.isfile(os.path.join(children_path, f))]

    return page_list

@activity.defn
async def map_metadata_page(payload: dict) -> str:
    print(f"Map metadata page")
    return_val = map_page(json.dumps(payload))
    return return_val
    


