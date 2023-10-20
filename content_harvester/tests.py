import json
import os

from . import settings

from .by_collection import harvest_collection
from .by_registry_endpoint import harvest_endpoint
from .sample_data.nuxeo_harvests import nuxeo_complex_object_harvests

def main():
    mapped_path = settings.DATA_SRC['PATH']
    urls = [
        f"https://registry.cdlib.org/api/v1/rikoltimapper/{f}/?format=json"
        for f in os.listdir(mapped_path)
    ]
    for url in urls:
        harvest_endpoint(url)


def test_static_samples():
    harvests = [
        # oac_harvests[0], islandora_harvests[0],
        # nuxeo_harvests[0],
        nuxeo_complex_object_harvests[0],
        # nuxeo_complex_object_harvests[-1],
        # nuxeo_nested_complex_object_harvests[0]
    ]

    for harvest in harvests:
        print(f"tests.py: {json.dumps(harvest)}")
        status = harvest_collection(harvest)
        print(f"Content status: {status}")


if __name__ == "__main__":
    # import argparse
    # parser = argparse.ArgumentParser(
    #     description="Fetch content using our mapped metadata")
    # parser.add_argument('payload', help='json payload')
    # args = parser.parse_args(sys.argv[1:])
    main()
