import os
import boto3
import requests

NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']

class NuxeoFileFetcher(object):

    def __init__(self, collection_id):
        self.collection_id = collection_id
        # TODO: allow for fetching files for an arbitrary list of nuxeo objects?

    def fetch_content_files(self):
        """ fetch content file to store in s3 """
        print(f"fetching content files for collection {self.collection_id=}")

        self.source_urls = self.list_source_urls()
        self.stash_content_files(self.source_urls)


    def list_source_urls(self):
        """ get a list of urls to hit """
        # TODO: parse media.json file or equivalent once that's available
        source_urls = [
            {
                "url": "https://nuxeo.cdlib.org/Nuxeo/nxfile/default/dc38be22-3f85-4e32-9e5e-a2f3763ccfb0/file:content/curivsc_ms272_001_005.pdf",
                "calisphere_id": "27414--dc38be22-3f85-4e32-9e5e-a2f3763ccfb0",
                "filename": "curivsc_ms272_001_005.pdf",
            },
            {
                "url": "https://nuxeo.cdlib.org/Nuxeo/nxfile/default/0b6bbcfa-c662-4c37-993a-2d54db1288a8/file:content/curivsc_ms272_001_002.pdf",
                "calisphere_id": "27414--0b6bbcfa-c662-4c37-993a-2d54db1288a8",
                "filename": "curivsc_ms272_001_002.pdf",
            },
        ]

        return source_urls

    def stash_content_files(self, source_urls):
        """ stash content files on s3 """
        # https://amalgjose.com/2020/08/13/python-program-to-stream-data-from-a-url-and-write-it-to-s3/
        s3_bucket = 'barbarahui_test_bucket'
        s3 = boto3.client('s3')
        requests_session = requests.Session()
        for url in source_urls:
            source_url = url.get('url')
            calisphere_id = url.get('calisphere_id')
            filename = url.get('filename')

            s3_key = f"content-files/{calisphere_id}::{filename}"
            print(f"{s3_key=}")
            response = requests_session.post(source_url,
                    auth=(NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
                    stream=True)
            with response as part:
                part.raw.decode_content = True
                conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                s3.upload_fileobj(part.raw, s3_bucket, s3_key, Config=conf)
