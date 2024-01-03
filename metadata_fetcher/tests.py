import argparse
import json
import logging

from . import settings
from .fetch_registry_collections import fetch_endpoint
from .lambda_function import fetch_collection
from .sample_data.nuxeo_harvests import (nuxeo_complex_object_harvests,
                                        nuxeo_harvests,
                                        nuxeo_nested_complex_object_harvests)
from .sample_data.oac_harvests import oac_harvests
from .sample_data.oai_harvests import oai_harvests
from rikolti.utils.versions import create_vernacular_version


def main():
    harvests = [
        oac_harvests[0], oai_harvests[0]
    ]

    if settings.NUXEO_TOKEN:
        harvests = harvests + [
            nuxeo_harvests[0], nuxeo_complex_object_harvests[0],
            nuxeo_nested_complex_object_harvests[0]
        ]

    for harvest in harvests:
        print(f"tests.py: {json.dumps(harvest)}")
        vernacular_version = create_vernacular_version(harvest.get('collection_id'))
        status = fetch_collection(json.dumps(harvest), vernacular_version)
        print(f"Harvest status: {status}")

    urls = [
        # harvest type = OAC
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&harvest_type=OAC",
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=oac_dc&offset=5",
        "https://registry.cdlib.org/api/v1/rikoltifetcher/9/?format=json",
        # harvest type = OAI
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&harvest_type=OAI",
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=islandora_oai_dc",
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=cca_vault_oai_dc",
        "https://registry.cdlib.org/api/v1/rikoltifetcher/26773/?format=json"
    ]

    if settings.NUXEO_TOKEN:
        urls = urls + [
            # harvest type = NUX
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&harvest_type=NUX",
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=ucldc_nuxeo",
            "https://registry.cdlib.org/api/v1/rikoltifetcher/22/?format=json",
        ]

    for url in urls:
        fetch_endpoint(url, 1)


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
