import argparse
import sys

import requests

from . import settings


def main(index):

    # create the API request
    url = f"{settings.ENDPOINT}/{index}"

    r = requests.delete(url, auth=settings.AUTH)
    r.raise_for_status()
    print(r.text)
    print(f"deleted index `{index}`")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete an index")
    parser.add_argument('index', help="Index name")
    args = parser.parse_args()

    sys.exit(main(args.index))
