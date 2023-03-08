import requests
import logging
import json
from lambda_shepherd import harvest_collection_content
import sys

def harvest_content_by_endpoint(url):
    collection_page = url
    results = []

    while collection_page:
        try:
            response = requests.get(url=collection_page)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg = (
                f"[{collection_page}]: "
                f"{err}; A valid collection id is required for mapping"
            )
            print(msg)
            collection_page = None
            break
        total_collections = response.json().get(
            'meta', {}).get('total_count', 1)
        print(
            f">>> Fetching content for {total_collections} collections "
            f"described at {collection_page}"
        )

        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        logging.debug(f"Next page: {collection_page}")
        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            log_msg = f"[{collection['collection_id']}]: " + "{}"
            print(log_msg.format(
                f"Harvesting content for collection {collection['collection_id']} - "
                f"{collection['solr_count']} items in solr as of "
                f"{collection['solr_last_updated']}"
            ))
            print(log_msg.format(f"lambda payload: {collection}"))
            # try:
            collection['mapper_type'] = collection['rikolti_mapper_type']
            return_val = harvest_collection_content(
                json.dumps(collection), None)
            # except KeyError:
            #     print(f"[{collection['collection_id']}]: {collection['mapper_type']} not yet implemented")
            #     continue
            # except FileNotFoundError:
            #     print(f"[{collection['collection_id']}]: not mapped yet")
            #     continue
            results.append(return_val)

            print(log_msg.format(f"{json.dumps(return_val)}"))

    # print(json.dumps(results))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content using mapped metadata")
    parser.add_argument(
        'url', 
        help="https://registry.cdlib.org/api/v1/rikoltimapper/<COLLECTION_ID>/?format=json"

    )
    args = parser.parse_args(sys.argv[1:])
    harvest_content_by_endpoint(args.url)
