import sys
from file_fetchers.file_fetcher import FileFetcher
from file_fetchers.nuxeo_file_fetcher import NuxeoFileFetcher

""" fetch content files for a given collection """
def main():
    collection_id = '27414'
    harvest_type = 'nuxeo'
    metadata_source = ''
    run_date = ''

    if harvest_type:
        file_fetcher = NuxeoFileFetcher(collection_id)
        file_fetcher.fetch_content_files()

    else:
        print(f"bad harvest type: {harvest_type}")
        

if __name__ == "__main__":

    sys.exit(main())