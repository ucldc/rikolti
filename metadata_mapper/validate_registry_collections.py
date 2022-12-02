import os
import requests
import argparse
import json
import sys
import logging
import settings
import urllib3
from datetime import datetime
from validate_mapping import validate_mapped_collection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def validate_collections(url):
    collection_page = url
    results = []

    while collection_page:
        response = requests.get(url=collection_page)
        response.raise_for_status()
        total_collections = response.json().get('meta', {}).get('total_count')
        print(
            f">>> Validating {total_collections} collections "
            f"described at {collection_page}"
        )

        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        logging.debug(f"Next page: {collection_page}")
        collections = response.json().get('objects')
        for collection in collections:
            collection_id = collection['collection_id']
            log_msg = f"[{collection_id}]: " + "{}"
            print(log_msg.format(
                f"Validating collection {collection_id} - "
                f"{collection['solr_count']} items in solr as of "
                f"{collection['solr_last_updated']}"
            ))
            logging.debug(log_msg.format(f"lambda payload: {collection}"))
            try:
                collection_validation = validate_mapped_collection(
                    json.dumps(collection))
            except FileNotFoundError:
                print(f"[{collection_id}]: not fetched yet")
                continue
            results.append(collection_validation)

            validation_path = settings.local_path('validation', collection_id)
            if not os.path.exists(validation_path):
                os.makedirs(validation_path)
            page_path = os.sep.join([
                validation_path,
                f"{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
            ])
            output = open(page_path, "w")
            for field_validation in collection_validation:
                output.write(field_validation)
                output.write('\n')
            output.close()

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    validation_errors = validate_collections(args.endpoint)
    print(validation_errors)
    sys.exit(0)
