import argparse
import sys
import lambda_function
import logging

import sys
sys.path.append('../')
from common.rikolti_utilities import registry_list

def fetch_endpoint(url, limit=None):
    results = []
    for collection in registry_list(url, limit):
        log_msg = f"[{collection['collection_id']}]: " + "{}"
        print(log_msg.format(
            f"Fetching collection {collection['collection_id']} - "
            f"{collection['solr_count']} items in solr as of "
            f"{collection['solr_last_updated']}"
        ))
        logging.debug(log_msg.format(f"lambda payload: {collection}"))
        return_val = lambda_function.fetch_collection(
            collection, None)
        results.append(return_val)

        if return_val['statusCode'] != 200:
            print(log_msg.format(f"Error: {return_val}"))
        else:
            print(log_msg.format(f"Fetch successful: {return_val}"))

    print(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run fetcher for registry endpoing')
    parser.add_argument(
        'endpoint', help='registry api endpoint or collection id')
    parser.add_argument(
        '--limit', help='limit the number of collections to fetch', type=int)
    args = parser.parse_args()

    if args.endpoint.isdigit():
        args.endpoint = (
            f"https://registry.cdlib.org/api/v1/rikoltifetcher/{args.endpoint}/"
            "?format=json"
        )

    fetch_endpoint(args.endpoint, args.limit)
    sys.exit(0)
