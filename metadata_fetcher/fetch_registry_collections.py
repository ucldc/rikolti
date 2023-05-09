import requests
import argparse
import json
import sys
import lambda_function
import logging


def fetch_endpoint(url, limit=None):

    collection_page = url
    results = {}

    while collection_page and (not limit or len(results) < limit):
        response = requests.get(url=collection_page)
        response.raise_for_status()
        total_collections = response.json().get('meta', {}).get('total_count', 1)
        print(
            f">>> Fetching {total_collections} collections "
            f"described at {collection_page}"
        )

        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        logging.debug(f"Next page: {collection_page}")
        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            if limit and len(results) >= limit:
                break
            log_msg = f"[{collection['collection_id']}]: " + "{}"
            print(log_msg.format(
                f"Fetching collection {collection['collection_id']} - "
                f"{collection['solr_count']} items in solr as of "
                f"{collection['solr_last_updated']}"
            ))
            logging.debug(log_msg.format(f"lambda payload: {collection}"))
            return_val = lambda_function.fetch_collection(
                collection, None)
            results[collection['collection_id']] = return_val

            if return_val[-1]['status'] != 'success':
                print(log_msg.format(f"Error: {return_val}"))
            else:
                logging.debug(log_msg.format(f"Fetch successful: {return_val}"))
        collection_page = False

    for collection_id, collection_result in results.items():
        success = all([page['status'] == 'success' for page in collection_result])
        total_items = sum([page['document_count'] for page in collection_result])
        total_pages = collection_result[-1]['page'] + 1
        print(
            f"[{collection_id}]: Fetch {'successful' if success else 'errored'} - "
            f"Fetched {total_items} items over {total_pages} pages"
        )
        if not success:
            print(f"[{collection_id}]: {collection_result[-1]}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run fetcher for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    fetch_endpoint(args.endpoint)
    sys.exit(0)
