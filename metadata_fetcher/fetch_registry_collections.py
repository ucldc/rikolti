import argparse
import logging
import sys

import requests

from . import lambda_function

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


def fetch_endpoint(url, limit=None):
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    fetch_report_headers = (
        "Collection ID, Status, Total Pages, Rikolti Count, Solr Count, "
        "Diff Count, Solr Last Updated"
    )

    if not limit:
        limit = total

    print(f">>> Fetching {limit}/{total} collections described at {url}")
    print(fetch_report_headers)

    results = {}

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']

        progress = progress + 1
        sys.stderr.write('\r')
        progress_bar = f"{progress}/{limit}"
        sys.stderr.write(
            f"{progress_bar:<9}: start fetching {collection_id:<6}")
        sys.stderr.flush()

        logger.debug(
            f"[{collection_id}]: call lambda with payload: {collection}")

        fetch_result = lambda_function.fetch_collection(collection, None)
        results[collection_id] = fetch_result

        success = all([page['status'] == 'success' for page in fetch_result])
        total_items = sum([page['document_count'] for page in fetch_result])
        total_pages = fetch_result[-1]['page'] + 1
        diff_items = total_items - collection['solr_count']

        fetch_report_row = (
            f"{collection_id}, {'success' if success else 'error'}, "
            f"{total_pages}, {total_items}, {collection['solr_count']}, "
            f"{diff_items}, {collection['solr_last_updated']}"
        )
        print(fetch_report_row)

        if not success:
            fetch_report_failure_row = (
                f"{collection_id}, {fetch_result[-1]}, -, -, -, -, -")
            print(fetch_report_failure_row)

        sys.stderr.write('\r')
        progress_bar = f"{progress}/{limit}"
        sys.stderr.write(
            f"{progress_bar:<9}: finish fetching {collection_id:<5}")
        sys.stderr.flush()

        if limit and len(results.keys()) >= limit:
            break

    sys.stderr.write('\n')
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
