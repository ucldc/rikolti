from datetime import datetime
import json
import os
import sys

import requests

def main():
    ''' combine all indices aliased to `rikolti-stg` into one large index '''
    endpoint = os.environ.get("OPENSEARCH_ENDPOINT")
    auth = (os.environ.get("OPENSEARCH_USER"), os.environ.get("OPENSEARCH_PASS"))
    version = datetime.today().strftime("%Y%m%d%H%M%S")
    combined_index = f"rikolti-stg-combined-{version}"

    # Get list of indices aliased to `rikolti-stg`
    url = f"{endpoint}/_alias/rikolti-stg"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    indices_to_combine = [key for key in r.json().keys()]
    print(f"Number of indices to combine: {len(indices_to_combine)}")

    count = 0
    for index in indices_to_combine:
        print(f"Adding {index} to {combined_index}")
        data = {
            "source":{
                "index": index
            },
            "dest":{
                "index": combined_index
            }
        }
        url = f"{endpoint}/_reindex"
        headers = {"Content-Type": "application/json"}

        r = requests.post(url, headers=headers, data=json.dumps(data), auth=auth)
        print(f"{r.json()}\n")
        r.raise_for_status()

        count += 1
    
    print(f"Combined {count} indices out of a total {len(indices_to_combine)} into {combined_index}")

if __name__ == "__main__":
    main()
    sys.exit()