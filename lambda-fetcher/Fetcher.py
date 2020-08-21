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
    def __init__(self, params):
        # log, validate parameters
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.write_page = params.get('write_page', 0)
        self.s3_data = {
            "ACL": 'bucket-owner-full-control',
            "Bucket": 'amy-test-bucket',
            "Key": f"{self.collection_id}/"
        }
        if not self.collection_id:
            print('no collection id!')

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
    async def fetchtos3(self): 
        async with aioboto3.client("s3") as s3_client:
            async with aiohttp.ClientSession() as http_client:

                next_page = await self.build_fetch_request()

                while next_page:
                    async with http_client.get(**next_page) as response:
                        records = await self.get_records(response)
                        jsonl = "\n".join([json.dumps(record) for record in records])

                        try:
                            await s3_client.put_object(
                                ACL=self.s3_data['ACL'],
                                Bucket=self.s3_data['Bucket'],
                                Key=(
                                    f"{self.s3_data['Key']}"
                                    f"{time.strftime('%Y-%m-%d')}/"
                                    f"{self.write_page}.jsonl"
                                ),
                                Body=jsonl)
                        except Exception as e:
                            print(e)

                        await self.increment(response)
                        next_page = await self.build_fetch_request()

    async def fetchtolocal(self):
        async with aiohttp.ClientSession() as http_client:
            next_page = await self.build_fetch_request()

            path = os.path.join(os.getcwd(),f"{self.collection_id}")
            if not os.path.exists(path):
                os.mkdir(path)

            while next_page:
                async with AIOFile(f"{self.collection_id}/{self.write_page}.jsonl", "w+") as afp:
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

    # uses self to build a URL for the fetch request
    # returns **kwargs for use in http_client.get()
    async def build_fetch_request(self):
        pass

    # takes an httpResponse and returns an array of records in dict format
    async def get_records(self, httpResp):
        pass

    # takes an httpResponse and updates self with params for the next page
    async def increment(self):
        self.write_page = self.write_page + 1

    # returns params for the next function invocation
    async def json(self):
        pass

    def __str__(self):
        attrs = vars(self)
        return (', '.join("%s: %s" % item for item in attrs.items()))
