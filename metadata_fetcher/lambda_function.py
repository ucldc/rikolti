import importlib
import json
import logging
import sys

from .fetchers.Fetcher import Fetcher, InvalidHarvestEndpoint

logger = logging.getLogger(__name__)

def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        f".fetchers.{harvest_type}_fetcher", package="metadata_fetcher")
    fetcher_module_words = harvest_type.split('_')
    class_type = ''.join([word.capitalize() for word in fetcher_module_words])
    fetcher_class = getattr(fetcher_module, f"{class_type}Fetcher")
    if fetcher_class not in Fetcher.__subclasses__():
        raise Exception(
            f"Fetcher class {fetcher_class} not a subclass of Fetcher")
    return fetcher_class


# AWS Lambda entry point
def fetch_collection(payload, context):
    if isinstance(payload, str):
        payload = json.loads(payload)

    logger.debug(f"fetch_collection payload: {payload}")

    fetcher_class = import_fetcher(payload.get('harvest_type'))

    fetch_report = {'page': payload.get('write_page', 0), 'document_count': 0}
    try:
        fetcher = fetcher_class(payload)
        fetch_report['document_count'] = fetcher.fetch_page()
    except InvalidHarvestEndpoint as e:
        logger.error(e)
        fetch_report.update({
            'status': 'error',
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        })
        return [fetch_report]

    next_page = fetcher.json()
    fetch_report.update({
        'status': 'success',
        'next_page': next_page
    })

    fetch_report = [fetch_report]

    if not json.loads(next_page).get('finished'):
        fetch_report.extend(fetch_collection(next_page, {}))

    return fetch_report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(
        filename=f"fetch_collection_{args.payload.get('collection_id')}.log",
        encoding='utf-8',
        level=logging.DEBUG
    )
    print(f"Starting to fetch collection {args.payload.get('collection_id')}")
    fetch_collection(args.payload, {})
    print(f"Finished fetching collection {args.payload.get('collection_id')}")
    sys.exit(0)
