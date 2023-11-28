import argparse
import logging
import sys

import requests

from . import lambda_function
from rikolti.utils.versions import create_vernacular_version

logger = logging.getLogger(__name__)

def registry_endpoint(url):
    page = url
    while page:
        response = requests.get(url=page)
        response.raise_for_status()
        page = response.json().get('meta', {}).get('next', None)
        if page:
            page = f"https://registry.cdlib.org{page}"

        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            yield collection


def fetch_endpoint(url, limit=None, job_logger=logger):
    """
    returns a dictionary of collection ids and fetch results, where
    fetch results are a list of of dictionaries with the following keys:
    ex: 3433: [
            {
                document_count: int
                vernacular_filepath: path relative to collection id
                    ex: "3433/vernacular_version_1/data/1"
                status: 'success' or 'error'
            }
        ]
    """
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    # fetch_report_headers = (
    #     "Collection ID, Status, Total Pages, Rikolti Count, Solr Count, "
    #     "Diff Count, Solr Last Updated"
    # )

    if not limit:
        limit = total

    print(f">>> Fetching {limit}/{total} collections described at {url}")
    # print(fetch_report_headers)

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
        fetch_result = lambda_function.fetch_collection(
            collection, vernacular_version, None)
        results[collection_id] = fetch_result

        success = all([page['status'] == 'success' for page in fetch_result])
        total_items = sum([page['document_count'] for page in fetch_result])
        total_pages = len(fetch_result)
        diff_items = total_items - collection['solr_count']
        diff_items_label = ""
        if diff_items > 0:
            diff_items_label = 'new items'
        elif diff_items < 0:
            diff_items_label = 'lost items'
        else:
            diff_items_label = 'same extent'

        progress_bar = f"{progress}/{limit}"
        job_logger.debug(
            f"{collection_id:<6}: finish fetching {progress_bar} collections")

        fetch_report_row = (
            f"{collection_id:<6}: {'success,' if success else 'error,':9} "
            f"{total_pages:>4} pages, {total_items:>6} items, "
            f"{collection['solr_count']:>6} solr items, "
            f"{str(diff_items) + ' ' + diff_items_label + ',':>16} "
            f"solr count last updated: {collection['solr_last_updated']}"
        )
        print(fetch_report_row)

        if not success:
            fetch_report_failure_row = (
                f"{collection_id:<6}: {fetch_result[-1]}")
            print(fetch_report_failure_row)

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
