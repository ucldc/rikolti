import argparse
import json
import logging
import os

from . import settings
from .lambda_shepherd import map_collection
from .map_registry_collections import map_endpoint
from .sample_data.islandora_harvests import islandora_harvests
from .sample_data.nuxeo_harvests import (nuxeo_complex_object_harvests,
                                         nuxeo_harvests,
                                         nuxeo_nested_complex_object_harvests)
from .sample_data.oac_harvests import oac_harvests
from .validate_mapping import validate_collection
from .validate_registry_collections import validate_endpoint


def main():
    vernacular_path = settings.DATA_SRC["PATH"]
    urls = [
        f"https://registry.cdlib.org/api/v1/rikoltimapper/{f}/?format=json"
        for f in os.listdir(vernacular_path)
    ]
    for url in urls:
        map_endpoint(url)

    for url in urls:
        validate_endpoint(url)


def test_static_samples():
    harvests = [
        oac_harvests[0], islandora_harvests[0],
        nuxeo_harvests[0], nuxeo_complex_object_harvests[0],
        nuxeo_nested_complex_object_harvests[0]
    ]

    for harvest in harvests:
        print(f"tests.py: {json.dumps(harvest)}")
        status = map_collection(json.dumps(harvest), {})
        print(f"Map status: {status}")

    for harvest in harvests:
        print(f"validate mapping: {json.dumps(harvest)}")
        validate_collection(json.dumps(harvest))
        print(f"validated: {str(harvest)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-log',
        '--loglevel',
        default='warning',
        help='log level (default: warning)'
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    logging.info('logging now set up')
    main()
