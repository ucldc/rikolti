import argparse
import json
import sys

import requests

from .create_collection_index import update_alias_for_collection
from . import settings


def move_index_to_prod(collection_id: str):
    """
    Add current rikolti-stg index to rikolti-prd alias
    """
    url = f"{settings.ENDPOINT}/_alias/rikolti-stg"
    r = requests.get(url=url, auth=settings.AUTH)
    r.raise_for_status()
    indices = json.loads(r.text)
    indices_for_collection = [key for key in indices if key.startswith(f"rikolti-{collection_id}-")]
    
    if len(indices_for_collection) == 1:
        update_alias_for_collection("rikolti-prd", collection_id, indices_for_collection[0])
    elif len(indices_for_collection) > 1:
        print(f"{collection_id}: More than one index associated with `rikolti-stg` alias: `{indices_for_collection}`")
        return
    elif len(indices_for_collection) < 1:
        print(f"{collection_id}: Cannot find any indices associated with `rikolti-stg` alias")
        return
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add staged index to production")
    parser.add_argument('collection_id', help='Registry collection ID')
    args = parser.parse_args(sys.argv[1:])
    move_index_to_prod(args.collection_id)
    sys.exit(0)