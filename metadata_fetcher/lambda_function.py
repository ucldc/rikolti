import importlib
import json
import logging
import sys

from .fetchers.Fetcher import Fetcher, InvalidHarvestEndpoint
from rikolti.utils.versions import create_vernacular_version

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
def fetch_collection(payload, vernacular_version) -> list[dict]:
    """
    returns a list of dicts with the following keys:
        document_count: int
        vernacular_version: path relative to collection id
            ex: "3433/vernacular_version_1/data/1"
        status: 'success' or 'error'
    """
    if isinstance(payload, str):
        payload = json.loads(payload)

    logger.debug(f"fetch_collection payload: {payload}")

    fetcher_class = import_fetcher(payload.get('harvest_type'))
    payload.update({'vernacular_version': vernacular_version})
    next_page = payload
    fetch_status = []

    while not next_page.get('finished'):
        fetcher = fetcher_class(next_page)
        page_status = fetcher.fetch_page()
        fetch_status.append(page_status)

        # this is a ucd json fetcher workaround
        if len(fetch_status) == 1 and isinstance(fetch_status[0], list):
            fetch_status = fetch_status[0]

        next_page = json.loads(fetcher.json())
        next_page.update({'vernacular_version': vernacular_version})

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
    fetch_collection(payload, vernacular_version)
    print(f"Finished fetching collection {payload.get('collection_id')}")
    sys.exit(0)
