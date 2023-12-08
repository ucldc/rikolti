import sys

import requests

from .by_collection import harvest_collection
from rikolti.utils.versions import get_most_recent_mapped_version

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


def harvest_endpoint(url, limit=None):
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    if not limit:
        limit = total
    print(
        f">>> Content harvest for {limit/total} collections described at {url}"
    )
    results = []

    for collection in registry_endpoint(url):
        print(
            f"{collection['id']:<6}: {collection['solr_count']} items in solr "
            f"as of {collection['solr_last_updated']}"
        )

        # TODO: what is return val? 
        collection_stats = harvest_collection(
            collection
            get_most_recent_mapped_version(collection['id'])
        )
        collection_stats.update({'solr_count': collection['solr_count']})
        results.append(collection_stats)

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
