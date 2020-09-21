import asyncio
import time
import json
import aiohttp
import botocore

import os
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    # TODO: standardize on aiofiles (used once in file-fetcher)
    # get rid of aiofile (used in lambda-fetcher & file-fetcher)
    from aiofile import AIOFile, Writer, LineReader
    import aiofiles
    import os
else:
    import aioboto3

import aioboto3

BASIC_USER = os.environ['NUXEO_USR']
BASIC_AUTH = os.environ['NUXEO_PASS']
AMZ_ID = os.environ['AMZ_ID']
AMZ_SECRET = os.environ['AMZ_SECRET']

class FileFetcher(object):
    def __init__(self, params):
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.metadata_date = params.get('metadata_date')
        self.start_page = params.get('start_page', 0)
        self.start_line = params.get('start_line', 0)

        print(f"content file fetcher for {self.collection_id} for metadata fetched on {self.metadata_date}; page: {self.start_page}, line: {self.start_line}")

        self.page = None
        self.line = None

        self.metadata_prefix = f"{self.collection_id}/{self.metadata_date}/"
        self.prefix = f"{self.collection_id}-files/{time.strftime('%Y-%m-%d')}/"


    async def list_metadata_pages(self, s3_client):
        pages = await s3_client.list_objects_v2(
            Bucket="amy-test-bucket", 
            Prefix=self.metadata_prefix)
        
        # strip off .jsonl for sorting/tracking page
        pages = [page['Key'][len(self.metadata_prefix):-6] for page in pages['Contents']]
        pages.sort(key=int)

        if self.start_page:
            pages = pages[pages.index(self.start_page):]
        return pages

    async def create_content_file_key(self, nuxeo_url, metadata):
        key = None
        print(nuxeo_url)
        filename = nuxeo_url.split('/')[-1]

        calisphere_id = metadata.get('calisphere-id')

        if filename and calisphere_id:
            key = (
                f"{self.prefix}{calisphere_id}-{filename}"
            )
        return key

    async def can_textract(self, content):
        length = content.get('length')
        length = int(length)/1000000 if length else None
        name = content.get('name')
        mime_type = content.get('mime-type')

        textract = False
        if name and mime_type and length:
            if mime_type == "application/pdf" and length <= 500:
                textract = True
            elif mime_type in ["image/png", "image/jpeg"] and length <= 10:
                textract = True

        return textract


    async def get_content_file_url(self, metadata):
        url = None

        if not metadata.get('uid') or not metadata.get('properties'):
            print(f"no uid or properties: {metadata.get('uid')} - {self.page}, {self.line}")
            return None

        if metadata['properties'].get('file:content') and await self.can_textract(
            metadata['properties']['file:content']):

            filename = metadata['properties']['file:content']['name']
            url = (
                f"https://nuxeo.cdlib.org/Nuxeo/nxfile/"
                f"default/{metadata.get('uid')}/file:content/{filename}"
            )

        elif metadata['properties'].get('picture:views'):
            picture_views = metadata['properties'].get('picture:views')
            textractable = [x['content'] for x in picture_views if x.get(
                'content') and await self.can_textract(x.get('content'))]

            if len(textractable) > 0:
                textractable.sort(key=lambda x: x['length'], reverse=True)
                url = (
                    f"https://nuxeo.cdlib.org/N"
                    f"{textractable[0]['data'][25:]}"
                )

        if not url:
            print(f"no url for: {metadata.get('uid')} - {self.page}, {self.line}")

        return url

    async def fetch_content_files(self):
        async with aiohttp.ClientSession() as nuxeo_http_client, aioboto3.client("s3",
            aws_access_key_id=AMZ_ID, 
            aws_secret_access_key=AMZ_SECRET) as s3_client:

            self.pages = await self.list_metadata_pages(s3_client)

            for page in self.pages:
                self.page = page
                metadata_file = await s3_client.get_object(Bucket='amy-test-bucket', 
                    Key=f"{self.metadata_prefix}{page}.jsonl")

                self.line = 0
                async for record in metadata_file['Body'].iter_lines():
                    print(self.line)
                    if self.start_line and self.line < self.start_line:
                        print(f"line {self.line} already processed")
                        self.line += 1
                        continue
                    else:
                        metadata = json.loads(record)

                        file_loc = await self.get_content_file_url(metadata)

                        if not file_loc:
                            print(f"NO FILE CONTENT {metadata.get('uid')} - {self.page},{self.line}")
                            self.line +=1
                            continue

                        s3_key = await self.create_content_file_key(file_loc, metadata)

                        # check if file already exists at destination
                        exists = True
                        try: 
                            await s3_client.head_object(Bucket='amy-test-bucket', 
                                Key=s3_key)
                            print(f"s3_key already exists {metadata.get('uid')} - {self.page},{self.line}")
                        except botocore.exceptions.ClientError as e:
                            if e.response['Error']['Code'] == "404":
                                exists = False
                        if not exists:
                            auth = aiohttp.BasicAuth(BASIC_USER, BASIC_AUTH)
                            async with nuxeo_http_client.get(file_loc, auth=auth) as response:
                                if response.status == 200:
                                    print(f"uploading: {s3_key}, {response.headers.get('Content-Length')}")
                                    await s3_client.upload_fileobj(response.content, 'amy-test-bucket', s3_key)
                                else:
                                    print(f"{response.status} - {file_loc}")
                        self.line += 1

    async def json(self):
        return json.dumps({
            'harvest_type': self.harvest_type,
            'collection_id': self.collection_id,
            'metadata_date': self.metadata_date,
            'start_page': self.page,
            'start_line': self.line
        })