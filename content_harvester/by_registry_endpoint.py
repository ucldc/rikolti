import sys

from rikolti.utils import registry_client
from rikolti.utils.versions import get_most_recent_mapped_version

from .by_collection import harvest_collection_content


def harvest_endpoint(url, limit=None):
    total = registry_client.collection_count(url)
    if not limit:
        limit = total
    print(
        f">>> Content harvest for {limit/total} collections described at {url}"
    )
    results = []

    for collection in registry_client.registry_endpoint(url):
        print(
            f"{collection['id']:<6}: {collection['solr_count']} items in solr "
            f"as of {collection['solr_last_updated']}"
        )

        # TODO: what is return val? 
        collection_stats = harvest_collection_content(
            collection['id'],
            collection['rikolti_mapper_type'],
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
