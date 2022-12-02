import requests
import argparse
import json
import sys
import lambda_shepherd
import logging


def map_endpoint(url):
    lookup = {
        'ucldc_nuxeo': 'nuxeo'
    }

    collection_page = url
    results = []

    while collection_page:
        response = requests.get(url=collection_page)
        response.raise_for_status()
        total_collections = response.json().get('meta', {}).get('total_count')
        print(
            f">>> Mapping {total_collections} collections "
            f"described at {collection_page}"
        )

        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        logging.debug(f"Next page: {collection_page}")
        collections = response.json().get('objects')
        for collection in collections:
            log_msg = f"[{collection['collection_id']}]: " + "{}"
            print(log_msg.format(
                f"Mapping collection {collection['collection_id']} - "
                f"{collection['solr_count']} items in solr as of "
                f"{collection['solr_last_updated']}"
            ))
            logging.debug(log_msg.format(f"lambda payload: {collection}"))
            try:
                collection['mapper_type'] = lookup[collection['mapper_type']]
                return_val = lambda_shepherd.map_collection(
                    json.dumps(collection), None)
            except FileNotFoundError:
                print(f"[{collection['collection_id']}]: not fetched yet")
                continue
            results.append(return_val)

            print(log_msg.format(f"{return_val}"))
        collection_page = False

    print(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    map_endpoint(args.endpoint)
    sys.exit(0)
