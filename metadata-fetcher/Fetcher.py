import asyncio
import time
import json
import aiohttp

import os
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    from aiofile import AIOFile, Writer
    import os
else:
    import aioboto3


class Fetcher(object):
    """Base Fetcher Class

    Fetcher always maintains current state, so if a timeout occurs, a new 
    fetcher can be produced from the json "bookmark" returned by Fetcher.json()

    The base fetcher class enforces the following lifecycle:
        - build API URL from the current state (including current page)
        - hit contributing institution API
        - store records from the API in s3 or local filesystem in jsonl format
        - increment the fetcher's current state given the API response

    Subclasses must implement build_fetch_request, get_records, increment, and json
    """
    def __init__(self, params):
        """Initialize Fetcher 

        Params should, at least, contain:
        {"harvest_type": str, "collection_id": int, "write_page": int}
        Params will necessarily contain further data regarding the 
        contributing institution's fetch endpoint; subclasses should handle
        this initialization. 
        """
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


    async def fetchtos3(self): 
        """fetch metadata records from remote API to s3

        records are stored in json line format at 
        s3:amy-test-bucket/<collection id>/<date>/<page>.jsonl
        """
        async with aioboto3.client("s3") as s3_client:
            async with aiohttp.ClientSession() as http_client:

                next_page = await self.build_fetch_request()

                while next_page:
                    async with http_client.get(**next_page) as response:
                        records = await self.get_records(response)
                        jsonl = "\n".join(
                            [json.dumps(record) for record in records])

                        try:
                            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
                            await s3_client.put_object(
                                ACL=self.s3_data['ACL'],
                                Bucket=self.s3_data['Bucket'],
                                Key=(
                                    f"{self.s3_data['Key']}"
                                    f"{self.write_page}.jsonl"
                                ),
                                Body=jsonl)
                        except Exception as e:
                            print(e)

                        await self.increment(response)
                        next_page = await self.build_fetch_request()

    async def fetchtolocal(self):
        """fetch metadata records from remote API to local filesystem

        used if DEBUG flag is set
        records are stored in json line format at
        ./<collection id>/<date>/<page>.jsonl
        """
        async with aiohttp.ClientSession() as http_client:
            next_page = await self.build_fetch_request()

            path = os.path.join(os.getcwd(),f"{self.collection_id}")
            if not os.path.exists(path):
                os.mkdir(path)

            date_path = os.path.join(path,f"{time.strftime('%Y-%m-%d')}")
            if not os.path.exists(date_path):
                os.mkdir(date_path)

            while next_page:
                filename = (
                    f"{self.collection_id}/{time.strftime('%Y-%m-%d')}/"
                    f"{self.write_page}.jsonl"
                )
                async with AIOFile(filename, "w+") as afp:
                    writer = Writer(afp)

                    async with http_client.get(**next_page) as response:
                        records = await self.get_records(response)

                        jsonl = "\n".join([json.dumps(record) for record in records])
                        await writer(jsonl)
                        await writer("\n")
                        await afp.fsync()

                        await self.increment(response)
                        next_page = await self.build_fetch_request()

    async def fetch(self):
        if DEBUG:
            await self.fetchtolocal()
        else:
            await self.fetchtos3()

    async def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientSession.get
        """
        pass

    async def get_records(self, httpResp):
        """parse httpResp from institution API into a list of records

        should return a list of dictionaries which can easily be serialized 
        by json.dumps into json line format; takes as an argument:
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        pass

    async def increment(self, httpResp):
        """increment internal state for fetching the next page

        takes as an argument the httpResp from institution API call
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        self.write_page = self.write_page + 1

    async def json(self):
        """build json serialization of current state"""
        pass

    def __str__(self):
        """build string representation of current state"""
        attrs = vars(self)
        return (', '.join("%s: %s" % item for item in attrs.items()))
