import argparse
import logging
import sys

import requests

from . import lambda_shepherd

logger = logging.getLogger(__name__)


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


def map_endpoint(url, fetched_versions, limit=None):
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    # map_report_headers = (
    #     "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
    # )

    if not limit:
        limit = total

    print(f">>> Mapping {limit}/{total} collections described at {url}")
    # print(map_report_headers)
    map_report = []

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']

        progress = progress + 1
        progress_bar = f"{progress}/{limit}"
        print(
            f"{collection_id:<6}: start mapping {progress_bar:<9}")

        logger.debug(
            f"{collection_id:<6}: call lambda with collection_id: {collection_id}")

        try:
            vernacular_version = fetched_versions[str(collection_id)]
            map_result = lambda_shepherd.map_collection(
                collection_id, vernacular_version)
        except FileNotFoundError:
            print(f"{collection_id:<6}: not fetched yet", file=sys.stderr)
            continue

        pre_mapping = map_result.get('pre_mapping', [])
        if len(pre_mapping) > 0:
            print(
                f"{collection_id:<6}: {'pre-mapping enrichments':<24}: "
                f"\"{pre_mapping}\""
            )

        enrichments = map_result.get('enrichments', [])
        if len(enrichments) > 0:
            print(
                f"{collection_id:<6}, {'post-mapping enrichments':<24}: "
                f"\"{enrichments}\""
            )

        missing_enrichments = map_result.get('missing_enrichments', [])
        if len(missing_enrichments) > 0:
            print(
                f"{collection_id:<6}, {'missing enrichments':<24}: "
                f"\"{missing_enrichments}\""
            )
        exceptions = map_result.get('exceptions', {})
        if len(exceptions) > 0:
            for exception, couch_ids in exceptions.items():
                print(
                    f"{collection_id:<6}, {'enrichment errors':<24}: "
                    f"{len(couch_ids)} items: \"{exception}\""
                )
                print(
                    f"{collection_id:<6}, {'enrichment error records':24}: "
                    f"{len(couch_ids)} items: \"{couch_ids}\""
                )

        # "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
        success = 'success' if map_result['status'] == 'success' else 'error'

        extent = map_result['count']
        diff = extent - collection['solr_count']
        diff_items_label = ""
        if diff > 0:
            diff_items_label = 'new items'
        elif diff < 0:
            diff_items_label = 'lost items'
        else:
            diff_items_label = 'same extent'

        progress_bar = f"{progress}/{limit}"
        logger.debug(
            f"{collection_id:<6}: finish mapping {progress_bar}")

        map_report_row = (
            f"{collection_id:<6}, {success:9}, {extent:>6} items, "
            f"{collection['solr_count']:>6} solr items, "
            f"{str(diff) + ' ' + diff_items_label + ',':>16} "
            f"solr count last updated: {collection['solr_last_updated']}"
        )
        print(map_report_row)
        map_report.append(map_result)

        if limit and progress >= limit:
            break

    return map_report



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    map_endpoint(args.endpoint)
    sys.exit(0)
