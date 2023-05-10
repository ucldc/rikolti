import argparse
import json
import sys
import lambda_shepherd
import logging

import sys
sys.path.append('../')
from common.rikolti_utilities import registry_list

def map_endpoint(url, limit=None):
    results = []
    for collection in registry_list(url, limit):
        log_msg = f"[{collection['collection_id']}]: " + "{}"
        print(log_msg.format(
            f"Mapping collection {collection['collection_id']} - "
            f"{collection['solr_count']} items in solr as of "
            f"{collection['solr_last_updated']}"
        ))
        logging.debug(log_msg.format(f"lambda payload: {collection}"))
        try:
            return_val = lambda_shepherd.map_collection(
                collection, None)
        except KeyError:
            print(f"[{collection['collection_id']}]: {collection['rikolti_mapper_type']} not yet implemented")
            continue
        except FileNotFoundError:
            print(f"[{collection['collection_id']}]: not fetched yet")
            continue
        results.append(return_val)

        print(log_msg.format(f"{json.dumps(return_val)}"))

    print(json.dumps(results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument(
        'endpoint', help='registry api endpoint or collection id')
    parser.add_argument(
        '--limit', help='limit the number of collections to fetch', type=int)
    args = parser.parse_args(sys.argv[1:])

    if args.endpoint.isdigit():
        args.endpoint = (
            f"https://registry.cdlib.org/api/v1/rikoltimapper/{args.endpoint}/"
            "?format=json"
        )

    map_endpoint(args.endpoint, args.limit)
    sys.exit(0)
