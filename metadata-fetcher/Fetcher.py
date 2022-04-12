import json
import requests
import time
import os

DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
    import boto3


class FetchError(Exception):
    pass


class Fetcher(object):
    def __init__(self, params):
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.write_page = params.get('write_page', 0)
        bucket = os.environ.get('S3_BUCKET', False)
        self.s3_data = {
            "ACL": 'bucket-owner-full-control',
            "Bucket": bucket,
            "Key": f"vernacular_metadata/{self.collection_id}/"
        }
        if not self.collection_id:
           print('no collection id!')

    def fetchtolocal(self, records):
        path = self.get_local_path()

        print(
            f"---------------------------------\n"
            f"{self}\n"
            f"WRITE PAGE {self.write_page}"
        )
        filename = os.path.join(path, f"{self.write_page}.jsonl")
        f = open(filename, "w+")

        jsonl = "\n".join([json.dumps(record) for record in records])
        f.write(jsonl)
        f.write("\n")

    def get_local_path(self):
        path = os.path.join(os.getcwd(), f"{self.collection_id}")
        if not os.path.exists(path):
            os.mkdir(path)

        date_path = os.path.join(path, f"{time.strftime('%Y-%m-%d')}")
        if not os.path.exists(date_path):
            os.mkdir(date_path)      

        return date_path  

    def fetchtos3(self, records):
        s3_client = boto3.client('s3')
        s3_key = self.get_s3_key()

        jsonl = "\n".join([json.dumps(record) for record in records])
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
            s3_client.put_object(
                ACL=self.s3_data['ACL'],
                Bucket=self.s3_data['Bucket'],
                Key=(
                    f"{s3_key}"
                    f"{self.write_page}.jsonl"
                ),
                Body=jsonl)
        except Exception as e:
            print(e)

    def get_s3_key(self):
        return self.s3_data['Key']

    def fetch_page(self):
        page = self.build_fetch_request()
        response = requests.get(**page)
        response.raise_for_status()
        records = self.get_records(response)

        if len(records) > 0:
            if DEBUG:
                self.fetchtolocal(records)
            else:
                self.fetchtos3(records)

        self.increment(response)

    def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientSession.get
        """
        pass

    def get_records(self, http_resp):
        """parse http_resp from institution API into a list of records

        should return a list of dictionaries which can easily be serialized
        by json.dumps into json line format; takes as an argument:
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        pass

    def increment(self, http_resp):
        """increment internal state for fetching the next page

        takes as an argument the http_resp from institution API call
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
