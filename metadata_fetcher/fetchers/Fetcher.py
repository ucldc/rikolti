import logging
import os
import sys

import boto3
import requests

from .. import settings

logger = logging.getLogger(__name__)


class InvalidHarvestEndpoint(Exception):
    '''Raised when the harvest endpoint is invalid'''


class CollectionIdRequired(Exception):
    '''Raised when the collection id is invalid'''


class FetchError(Exception):
    pass


class Fetcher(object):
    def __init__(self, params):
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.write_page = params.get('write_page', 0)
        bucket = settings.DATA_DEST["BUCKET"]

        self.s3_data = {
            "ACL": 'bucket-owner-full-control',
            "Bucket": bucket,
            "Key": f"vernacular_metadata/{self.collection_id}/"
        }
        if not self.collection_id:
            raise CollectionIdRequired("collection_id is required")

    def fetchtolocal(self, page):
        path = self.get_local_path()

        filename = os.path.join(path, f"{self.write_page}")
        f = open(filename, "w+")

        f.write(page)

    def get_local_path(self):
        local_path = os.sep.join([
            settings.DATA_DEST["PATH"],
            'vernacular_metadata',
            str(self.collection_id),
        ])
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        return local_path

    def fetchtos3(self, page):
        s3_client = boto3.client('s3')
        s3_key = self.s3_data['Key']

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
            s3_client.put_object(
                ACL=self.s3_data['ACL'],
                Bucket=self.s3_data['Bucket'],
                Key=(
                    f"{s3_key}"
                    f"{self.write_page}"
                ),
                Body=page)
        except Exception as e:
            print(e, file=sys.stderr)

    def fetch_page(self):
        page = self.build_fetch_request()
        logger.debug(
            f"[{self.collection_id}]: fetching page {self.write_page} "
            f"at {page.get('url')}"
        )
        try:
            response = requests.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch page {page}")

        record_count = self.check_page(response)
        if record_count:
            content = self.aggregate_vernacular_content(response.text)
            if settings.DATA_DEST["STORE"] != 's3':
                self.fetchtolocal(content)
            else:
                self.fetchtos3(content)

        self.increment(response)

        return record_count

    def aggregate_vernacular_content(self, response):
        return response

    def build_fetch_request(self):
        """build parameters for the institution's requests.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.python-requests.org/en/latest/api/#requests.get
        """
        pass

    def get_records(self, http_resp):
        """parses http_resp from institutional API into a list of records

        should return a list of dictionaries which can easily be serialized
        by json.dumps into json line format; takes as an argument:
        https://docs.python-requests.org/en/latest/api/#requests.Response
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
