import importlib
import json
import logging
import sys

from .fetchers.Fetcher import Fetcher, InvalidHarvestEndpoint
from rikolti.utils.rikolti_storage import create_vernacular_version

logger = logging.getLogger(__name__)

def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        f".fetchers.{harvest_type}_fetcher", package=__package__)
    fetcher_module_words = harvest_type.split('_')
    class_type = ''.join([word.capitalize() for word in fetcher_module_words])
    fetcher_class = getattr(fetcher_module, f"{class_type}Fetcher")
    if fetcher_class not in Fetcher.__subclasses__():
        raise Exception(
            f"Fetcher class {fetcher_class} not a subclass of Fetcher")
    return fetcher_class


# AWS Lambda entry point
def fetch_collection(payload, vernacular_version, context):
    if isinstance(payload, str):
        payload = json.loads(payload)

    logger.debug(f"fetch_collection payload: {payload}")

    fetcher_class = import_fetcher(payload.get('harvest_type'))

    fetch_status = []
    try:
        fetcher = fetcher_class(payload, vernacular_version)
        fetch_status.append(fetcher.fetch_page())
    except InvalidHarvestEndpoint as e:
        logger.error(e)
        fetch_status.append({
            'status': 'error',
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        })
        return fetch_status

    next_page = fetcher.json()

    # this is a ucd json fetcher workaround
    # TODO: could be cleaner to stash ucd's table of contents in a known
    # location and have each iteration of the fetcher reference that location,
    # then we could resolve this difference in return values
    if len(fetch_status) == 1 and type(fetch_status[0]) == list:
        fetch_status = fetch_status[0]

    if not json.loads(next_page).get('finished'):
        fetch_status.extend(fetch_collection(next_page, vernacular_version, {}))

    return fetch_status


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    payload = json.loads(args.payload)

    logging.basicConfig(
        filename=f"fetch_collection_{payload.get('collection_id')}.log",
        encoding='utf-8',
        level=logging.DEBUG
    )
    vernacular_version = create_vernacular_version(payload.get('collection_id'))
    print(f"Starting to fetch collection {payload.get('collection_id')}")
    fetch_collection(payload, vernacular_version, {})
    print(f"Finished fetching collection {payload.get('collection_id')}")
    sys.exit(0)
