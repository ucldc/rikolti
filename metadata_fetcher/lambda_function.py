import importlib
import json
import logging
import sys

from .fetchers.Fetcher import Fetcher, InvalidHarvestEndpoint

logger = logging.getLogger(__name__)

def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        f".fetchers.{harvest_type}_fetcher", package=__package__)
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

    fetch_status = {'page': payload.get('write_page', 0), 'document_count': 0}
    try:
        fetcher = fetcher_class(payload)
        fetch_status['document_count'] = fetcher.fetch_page()
    except InvalidHarvestEndpoint as e:
        logger.error(e)
        fetch_status.update({
            'status': 'error',
            'body': json.dumps({
                'error': repr(e),
                'payload': payload
            })
        })
        return [fetch_status]

    next_page = fetcher.json()
    fetch_status.update({
        'status': 'success',
        'next_page': next_page
    })

    fetch_status = [fetch_status]

    if not json.loads(next_page).get('finished'):
        fetch_status.extend(fetch_collection(next_page, {}))

    return fetch_status


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    payload = json.loads(args.payload)

    logging.basicConfig(
        filename=f"fetch_collection_{payload.get('collection_id')}.log",
        encoding='utf-8',
        level=logging.DEBUG
    )
    print(f"Starting to fetch collection {payload.get('collection_id')}")
    fetch_collection(payload, {})
    print(f"Finished fetching collection {payload.get('collection_id')}")
    sys.exit(0)
