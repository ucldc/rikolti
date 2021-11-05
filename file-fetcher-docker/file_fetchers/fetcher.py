import os
import boto3
import botocore
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

S3_PUBLIC_BUCKET = os.environ['S3_PUBLIC_BUCKET']
#S3_PUBLIC_BUCKET = 'barbarahui_test_bucket'
S3_CONTENT_FILES_FOLDER = os.environ['S3_CONTENT_FILES_FOLDER']

class Fetcher(object):
    def __init__(self, collection_id, **kwargs):
        self.collection_id = collection_id
        if 'clean' in kwargs:
            self.clean_stash = kwargs['clean']
        else:
            self.clean_stash = False

        self.s3 = boto3.client('s3')

        self.set_request_session()

    def fetch_files(self):
        """ Fetch files for a collection and stash on s3

            Return json representation of what was stashed

            For most sources, this will just be one thumbnail per item
            For sources like Nuxeo, override this class to fetch other files
        """
        raise NotImplementedError

    def stash_content_file(self, id, filename, fetch_request):

        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{id}::{filename}"

        if self.clean_stash or not self.already_stashed(S3_PUBLIC_BUCKET, s3_key):
            # fetch file
            response = self.http.get(**fetch_request)
            response.raise_for_status()

            # stash file on s3
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html
            with response as part:
                part.raw.decode_content = True
                conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                self.s3.upload_fileobj(part.raw, S3_PUBLIC_BUCKET, s3_key, Config=conf)
            print(f"stashed on s3: s3://{S3_PUBLIC_BUCKET}/{s3_key}")

        return f"s3://{S3_PUBLIC_BUCKET}/{s3_key}"

    def stash_thumbnail(self):
        """ stash thumbnail files using md5s3stash """
        md5hash = None
        return md5hash

    def already_stashed(self, bucket, key):
        try:
            response = self.s3.head_object(
                Bucket=S3_PUBLIC_BUCKET,
                Key=key
            )
            print(f"already stashed: s3://{S3_PUBLIC_BUCKET}/{key}")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

    def set_request_session(self):
        retry_strategy = Retry(
            total=3,
            status_forcelist=[413, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        requests.get()
        """
        raise NotImplementedError

