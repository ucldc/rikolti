import requests
import json
from by_collection import harvest_collection
import sys


def harvest_endpoint(url):
    registry_page = url
    results = []

    while registry_page:
        try:
            response = requests.get(url=registry_page)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(
                f"[{registry_page}]: {err}"
            )
            registry_page = None
            break

        total_collections = response.json().get(
            'meta', {}).get('total_count', 1)
        print(
            f">>> Harvesting content for {total_collections} collections "
            f"described at {registry_page}"
        )

        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            print(
                f"> Harvesting content from collection "
                f"{collection['collection_id']} - {collection['solr_count']} "
                f"items in solr as of {collection['solr_last_updated']}"
            )

            # TODO: what is return val? 
            collection_stats = harvest_collection(collection)

            collection_stats.update({'solr_count': collection['solr_count']})

            results.append(collection_stats)

        print(f">>> Harvested {len(results)} collections")

        registry_page = response.json().get('meta', {}).get('next')
        if registry_page:
            registry_page = f"https://registry.cdlib.org{registry_page}"
        print(f">>> Next page: {registry_page}")

    return results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content using mapped metadata")
    parser.add_argument(
        'url', 
        help="https://registry.cdlib.org/api/v1/rikoltimapper/<COLLECTION_ID>/?format=json"

    )
    args = parser.parse_args(sys.argv[1:])
    print(harvest_endpoint(args.url))
