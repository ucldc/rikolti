import traceback
from pprint import pprint

import requests

def print_opensearch_error(r: requests.Response, url: str):
    direct_frame = traceback.extract_stack(limit=2)[0]
    print(f"ERROR @ {direct_frame.name} from {direct_frame.filename}")
    try:
        pprint(r.json())
    except requests.exceptions.JSONDecodeError:
        print(f"{r.status_code}: {url}")
        print(f"Response: {r.text}")
