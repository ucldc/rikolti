import sys
import argparse
from file_fetchers.fetcher import Fetcher
from file_fetchers.nuxeo_fetcher import NuxeoFetcher

""" fetch content files for a given collection """
def get_fetcher(collection_id, harvest_type, clean):
    try:
        globals()[harvest_type]
    except KeyError:
        print(f"{ harvest_type } not imported")
        exit()

    if globals()[harvest_type] not in Fetcher.__subclasses__():
        print(f"{ harvest_type } not a subclass of Fetcher")
        exit()

    try:
        fetcher = eval(harvest_type)(collection_id, clean=clean)
    except NameError:
        print(f"bad harvest type: { harvest_type }")
        exit()

    return fetcher

def main(collection_id, harvest_type, clean):

    fetcher = get_fetcher(collection_id, harvest_type, clean)

    fetcher.fetch_files()
    #next_item = fetcher.json()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('collection_id')
    parser.add_argument('harvest_type')
    parser.add_argument('--clean', action='store_true', help='clean restash')
    args = parser.parse_args()

    sys.exit(main(args.collection_id, args.harvest_type, clean=args.clean))