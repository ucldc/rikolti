import requests
import argparse
import json
import sys
import lambda_function
import logging


def fetch_endpoint(url, limit=None):

    collection_page = url
    results = []

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
                json.dumps(collection), None)
            results.append(return_val)

            if return_val['statusCode'] != 200:
                print(log_msg.format(f"Error: {return_val}"))
            else:
                print(log_msg.format(f"Fetch successful: {return_val}"))
        collection_page = False

    print(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run fetcher for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    fetch_endpoint(args.endpoint)
    sys.exit(0)
