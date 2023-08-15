import requests
import argparse
import sys
import lambda_shepherd
import logging
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

def map_endpoint(url, limit=None):
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    map_report_headers = (
        "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
    )

    if not limit:
        limit = total

    print(f">>> Mapping {limit}/{total} collections described at {url}")
    print(map_report_headers)

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']

        progress = progress + 1
        sys.stderr.write('\r')
        progress_bar = f"{progress}/{limit}"
        sys.stderr.write(
            f"{progress_bar:<9}: start mapping {collection_id:<6}")
        sys.stderr.flush()

        logger.debug(
            f"[{collection_id}]: call lambda with payload: {collection}")

        try:
            map_result = lambda_shepherd.map_collection(
                collection, None)
        except FileNotFoundError:
            print(f"[{collection_id}]: not fetched yet", file=sys.stderr)
            continue

        missing_enrichments = map_result.get('missing_enrichments')
        if len(missing_enrichments) > 0:
            print(
                f"{collection_id}, missing enrichments, ",
                f"ALL, -, -, {missing_enrichments}"
            )
        exceptions = map_result.get('exceptions')
        if len(exceptions) > 0:
            for exception, couch_ids in exceptions.items():
                print(
                    f"{collection_id}, enrichment errors, "
                    f'{len(couch_ids)}, -, -, "{exception}"'
                )
                print(
                    f"{collection_id}, enrichment error records, "
                    f'{len(couch_ids)}, -, -, "{couch_ids}"'
                )


        # "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
        success = 'success' if map_result['status'] == 'success' else 'error'
        extent = map_result['records_mapped']
        diff = extent - collection['solr_count']
        map_report_row = (
            f"{collection_id}, {success}, {extent}, "
            f"{collection['solr_count']}, {diff}, "
            f"{collection['solr_last_updated']}"
        )
        print(map_report_row)

        sys.stderr.write('\r')
        progress_bar = f"{progress}/{limit}"
        sys.stderr.write(
            f"{progress_bar:<9}: finish mapping {collection_id:<5}")
        sys.stderr.flush()

        if limit and progress >= limit:
            break

    sys.stderr.write('\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    map_endpoint(args.endpoint)
    sys.exit(0)
