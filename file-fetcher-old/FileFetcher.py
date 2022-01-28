import asyncio
import time
import json
import aiohttp
import aioboto3
import botocore

import os
BASIC_USER = os.environ['NUXEO_USR']
BASIC_AUTH = os.environ['NUXEO_PASS']

async def can_textract(content):
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


class FileFetcher(object):
    def __init__(self, params):
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.metadata_source = params.get('metadata_source')

        print((
            f"content file fetcher for {self.collection_id} for metadata "
            f"fetched on {self.metadata_source.get('date')}; "
            f"page: {self.metadata_source.get('start_page', 0)}, "
            f"line: {self.metadata_source.get('start_line', 0)}"
        ))

        # because image fetching can take awhile, 
        # wouldn't want this to wind up split across two different folders
        # probably should just use s3 document versioning though?
        self.run_date = params.get('run_date', time.strftime('%Y-%m-%d'))
        self.page = None
        self.line = None

        self.metadata_prefix =  f"{self.collection_id}/{self.metadata_source.get('date')}/"
        self.dest_prefix =      f"{self.collection_id}-files/{self.run_date}/"


    async def list_metadata_pages(self, s3_client):
        pages = await s3_client.list_objects_v2(
            Bucket="amy-test-bucket", 
            Prefix=self.metadata_prefix)
        
        if not 'Contents' in pages:
            print(f"No metadata files found at {self.metadata_prefix}")
            return []
        # strip off .jsonl for sorting/tracking page
        pages = [page['Key'][len(self.metadata_prefix):-6] for page in pages['Contents']]
        pages.sort(key=int)

        if self.metadata_source.get('start_page'):
            pages = pages[pages.index(self.metadata_source.get('start_page')):]
        return pages


    async def map_isShownBy(self, metadata):
        isShownBy = None

        if not metadata.get('uid') or not metadata.get('properties'):
            return None

        if metadata['properties'].get('file:content') and await can_textract(
            metadata['properties']['file:content']):

            filename = metadata['properties']['file:content']['name']
            isShownBy = (
                f"https://nuxeo.cdlib.org/Nuxeo/nxfile/"
                f"default/{metadata.get('uid')}/file:content/{filename}"
            )

        elif metadata['properties'].get('picture:views'):
            picture_views = metadata['properties'].get('picture:views')
            textractable = [x['content'] for x in picture_views if x.get(
                'content') and await can_textract(x.get('content'))]

            if len(textractable) > 0:
                textractable.sort(key=lambda x: x['length'], reverse=True)
                isShownBy = (
                    f"https://nuxeo.cdlib.org/N"
                    f"{textractable[0]['data'][25:]}"
                )

        return isShownBy


    async def fetch_content_files(self):
        """Fetch content files to store in s3 for later processing

        Fetch metadata file(s) from s3 - records stored in json line format
        For each record map the metadata to a content file URL
        Fetch content file(s) from Nuxeo and stash into s3
        """
        async with aiohttp.ClientSession() as nuxeo_http_client:
            async with aioboto3.client("s3") as s3_client:

                # TODO, these two for-loops could be a generator function; 
                # yield: page_name, line_number, line_content
                for page in await self.list_metadata_pages(s3_client):
                    self.page = page
                    metadata_page = await s3_client.get_object(
                        Bucket='amy-test-bucket', 
                        Key=f"{self.metadata_prefix}{page}.jsonl"
                    )

                    self.line = 0
                    async for record in metadata_page['Body'].iter_lines():
                        start_line = self.metadata_source.get('start_line')

                        if start_line and self.line < start_line:
                            print(f"skipping {self.line} - already processed")
                            self.line += 1
                            continue
                        
                        metadata = json.loads(record)
                        isShownBy = await self.map_isShownBy(metadata)
                        calisphere_id = metadata.get('calisphere-id')

                        if not isShownBy:
                            print((
                                f"no isShownBy for: {calisphere_id} - "
                                f"{self.page}, {self.line}"
                            ))
                            self.line +=1
                            continue

                        dest_key = (
                            f"{self.dest_prefix}{calisphere_id}::"
                            f"{isShownBy.split('/')[-1]}"
                        )

                        # check if file already exists at destination
                        try: 
                            await s3_client.head_object(
                                Bucket='amy-test-bucket', 
                                Key=dest_key
                            )
                            print((
                                f"destination already exists {calisphere_id} - "
                                f"{self.page},{self.line}"
                            ))
                            self.line += 1
                            continue

                        except botocore.exceptions.ClientError as e:
                            if e.response['Error']['Code'] == "404":
                                pass

                        # get file
                        # TODO - abstract this into function so it can be subclassed
                        # not all FileFetchers wil need basic auth
                        auth = aiohttp.BasicAuth(BASIC_USER, BASIC_AUTH)
                        async with nuxeo_http_client.get(isShownBy, 
                            auth=auth) as response:
                            if response.status == 200:
                                # stream file to s3
                                # https://aioboto3.readthedocs.io/en/latest/usage.html#upload
                                print((
                                    f"uploading: {dest_key}, "
                                    f"{response.headers.get('Content-Length')}"
                                ))
                                await s3_client.upload_fileobj(
                                    response.content, 
                                    'amy-test-bucket', 
                                    dest_key
                                )
                                await s3_client.put_object_acl(
                                    ACL='bucket-owner-full-control',
                                    Bucket='amy-test-bucket',
                                    Key=dest_key
                                )
                            else:
                                print(f"{response.status} - {isShownBy}")
                        self.line += 1


    async def json(self):
        return json.dumps({
            'harvest_type': self.harvest_type,
            'collection_id': self.collection_id,
            'metadata_source': {
                'date': self.metadata_source.get('date'),
                'start_page': self.page,
                'start_line': self.line
            },
            'run_date': self.run_date
        })

