import json
import requests
import time

import os
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    import os
else:
    import boto3

class FetchError(Exception):
    pass

class Fetcher(object):
    def __init__(self, params):
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.write_page = params.get('write_page', 0)
        self.s3_data = {
            "ACL": 'bucket-owner-full-control',
            "Bucket": 'amy-test-bucket',
            "Key": f"vernacular_metadata/{self.collection_id}/"
        }
        if not self.collection_id:
            print('no collection id!')


    def fetchtolocal(self):
        page = self.build_fetch_request()

        path = os.path.join(os.getcwd(),f"{self.collection_id}")
        if not os.path.exists(path):
            os.mkdir(path)

        date_path = os.path.join(path,f"{time.strftime('%Y-%m-%d')}")
        if not os.path.exists(date_path):
            os.mkdir(date_path)

        filename = (
            f"{self.collection_id}/{time.strftime('%Y-%m-%d')}/"
            f"{self.write_page}.jsonl"
        )
        f = open(filename, "w+")

        response = requests.get(**page)
        records = self.get_records(response)

        jsonl = "\n".join([json.dumps(record) for record in records])
        f.write(jsonl)
        f.write("\n")

        self.increment(response)

    def fetchPage(self):
        if DEBUG:
            self.fetchtolocal()
        # else:
        #     self.fetchtos3()

    def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientSession.get
        """
        pass

    def get_records(self, httpResp):
        """parse httpResp from institution API into a list of records

        should return a list of dictionaries which can easily be serialized 
        by json.dumps into json line format; takes as an argument:
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        pass

    def increment(self, httpResp):
        """increment internal state for fetching the next page

        takes as an argument the httpResp from institution API call
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        self.write_page = self.write_page + 1

    def json(self):
        """build json serialization of current state"""
        pass

    def __str__(self):
        """build string representation of current state"""
        attrs = vars(self)
        return (', '.join("%s: %s" % item for item in attrs.items()))
