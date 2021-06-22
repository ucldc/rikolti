import sys
import argparse
from file_fetchers.file_fetcher import FileFetcher
from file_fetchers.nuxeo_file_fetcher import NuxeoFileFetcher

""" fetch content files for a given collection """
def main(collection_id):
    # get collection type from registry and map to FileFetcher type
    harvest_type = 'nuxeo'

    if harvest_type:
        file_fetcher = NuxeoFileFetcher(collection_id)
        file_fetcher.fetch_content_files()

    else:
        print(f"bad harvest type: {harvest_type}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("collection_id")
    args = parser.parse_args()

    sys.exit(main(args.collection_id))