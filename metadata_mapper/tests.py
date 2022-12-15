from lambda_shepherd import map_collection
# from validate_mapping import validate_mapped_collection
from sample_data.nuxeo_harvests import nuxeo_harvests, \
    nuxeo_complex_object_harvests, nuxeo_nested_complex_object_harvests
from sample_data.oac_harvests import oac_harvests
from sample_data.islandora_harvests import islandora_harvests
import json
import argparse
import logging

def main():
    harvests = [
        # oac_harvests[0],
        islandora_harvests[0],
        nuxeo_harvests[0],
        nuxeo_complex_object_harvests[0],
        nuxeo_nested_complex_object_harvests[0]
    ]

    for harvest in harvests:
        print(f"mapping tests: {json.dumps(harvest)}")
        map_collection(json.dumps(harvest), {})
        print(f"mapped: {str(harvest)}")

    # for harvest in harvests:
    #     print(f"validate mapping: {json.dumps(harvest)}")
    #     validate_mapped_collection(json.dumps(harvest))
    #     print(f"validated: {str(harvest)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( '-log', '--loglevel', default='warning', help='log level (default: warning)')
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    logging.info('logging now set up')
    main()