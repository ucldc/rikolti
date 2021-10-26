import sys
import argparse
from file_fetchers.file_fetcher import FileFetcher
from file_fetchers.nuxeo_file_fetcher import NuxeoFileFetcher

""" fetch content files for a given collection """
def get_fetcher(collection_id, fetcher_type, clean):

    try:
        globals()[fetcher_type]
    except KeyError:
        print(f"{ fetcher_type } not imported")
        exit()

    if globals()[fetcher_type] not in FileFetcher.__subclasses__():
        print(f"{ fetcher_type } not a subclass of Fetcher")
        exit()

    try:
        fetcher = eval(fetcher_type)(collection_id, fetcher_type, clean)
    except NameError:
        print(f"bad file fetcher type: { fetcher_type }")
        exit()

    return fetcher

def main(collection_id, fetcher_type, clean):

    file_fetcher = get_fetcher(collection_id, fetcher_type, clean)
    file_fetcher.fetch_content_files()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('collection_id')
    parser.add_argument('fetcher_type')
    parser.add_argument('--clean', action='store_true', help='clean restash')
    args = parser.parse_args()

    sys.exit(main(args.collection_id, args.fetcher_type, args.clean))