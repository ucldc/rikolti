import sys
import argparse
import json
from by_collection import index_collection


def run_indexer(payload):
    if isinstance(payload, str):
        payload = json.loads(payload)

    collection_id = payload.get('collection_id')
    print(collection_id)

    index_collection(collection_id)

    #what should output be?


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add mapped metadata to the OpenSearch index")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    run_indexer(args.payload, {})
    sys.exit(0)