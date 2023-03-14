import sys, os
import settings
import requests
import argparse

ENDPOINT = os.environ.get('RIKOLTI_ES_ENDPOINT')
AUTH = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

def main(index):

    # create the API request
    url = os.path.join(ENDPOINT, index)

    # create index template
    r = requests.delete(url, auth=AUTH)
    r.raise_for_status()
    print(r.text)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Delete an index")
    parser.add_argument('index', help="Index Name")
    args = parser.parse_args()

    sys.exit(main(args.index))