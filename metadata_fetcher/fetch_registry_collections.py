import argparse
import logging
import sys

import requests

from . import lambda_function
from rikolti.utils.versions import create_vernacular_version
from rikolti.utils.registry_client import registry_endpoint

logger = logging.getLogger(__name__)


def fetch_endpoint(
        url, 
        limit=None, 
        job_logger=logger) -> dict[int, lambda_function.FetchedCollectionStatus]:
    """
    returns a dictionary of collection ids and FetchedCollectionStatus objects:
    ex: {
        3433: FetchedCollectionStatus(
            num_items=10,
            num_pages=1,
            num_parent_items=10,
            num_parent_pages=1,
            filepaths=['3433/vernacular_version_1/data/1'],
            children=False,
            version='3433/vernacular_version_1'
        )
    }
    """
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    if not limit:
        limit = total
    else:
        limit = int(limit)

    print(f">>> Fetching {limit}/{total} collections described at {url}")

    results = {}

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']

        progress = progress + 1
        progress_bar = f"{progress}/{limit}"
        job_logger.debug(
            f"{collection_id:<6}: start fetching {progress_bar} collections")

        job_logger.debug(
            f"{collection_id:<6}: call lambda with payload: {collection}")

        vernacular_version = create_vernacular_version(collection_id)

        try:
            fetched_collection = lambda_function.fetch_collection(
                collection, vernacular_version)
        except Exception as e:
            print(f"ERROR fetching collection { collection_id }: {e}")
            results[collection_id] = {
                'status': 'error',
                'error_message': e
            }
            continue

        results[collection_id] = fetched_collection
        lambda_function.print_fetched_collection_report(
            collection, fetched_collection)

        progress_bar = f"{progress}/{limit}"
        job_logger.debug(
            f"{collection_id:<6}: finish fetching {progress_bar} collections")

        if limit and len(results.keys()) >= limit:
            break

    return results


if __name__ == "__main__":
    logging.basicConfig(
        filename='fetch_endpoint.log', encoding='utf-8', level=logging.DEBUG)
    parser = argparse.ArgumentParser(
        description="Run fetcher for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    fetch_endpoint(args.endpoint)
    sys.exit(0)
