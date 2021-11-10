import os
import sys
import argparse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib.parse import urlparse
import base64
import boto3
from boto3.s3.transfer import TransferConfig
import botocore
import tempfile
import hashlib
from PIL import Image
from PIL import UnidentifiedImageError
import magic

S3_THUMBNAIL_BUCKET = os.environ.get('S3_THUMBNAIL_BUCKET')
S3_THUMBNAIL_FOLDER = os.environ.get('s3_THUMBNAIL_FOLDER')
BASIC_USER = os.environ.get('BASIC_USER')
BASIC_PASS = os.environ.get('BASIC_PASS')
TIMEOUT_CONNECT = 12.05
TIMEOUT_READ = (60 * 10) + 0.05

class md5s3stash(object):
    ''' fetch an image and stash on s3 using md5hash as key '''
    def __init__(self, **kwargs):

        self.url = None
        self.localpath = None
        self.basic_auth = False

        if 'url' in kwargs:
            self.url = kwargs['url']

        if 'localpath' in kwargs:
            self.localpath = kwargs['localpath']

        if self.url is None and self.localpath is None:
            raise TypeError("Must provide either url or localpath.")

        if self.url and self.localpath:
            print(f"Both url and localpath provided! Uploading from localpath.")

        if BASIC_USER or BASIC_PASS:
            self.basic_auth = True

        self.s3 = boto3.client('s3')
        self.md5hash = None

    def stash(self):
        if self.localpath:
            self.stash_local()
        else:
            self.stash_remote()

    def stash_local(self):

        self.get_file_info()

        if not self.is_image:
            print(f"File is not of type image. Not stashing: {self.localpath}")
            return

        # get md5hash
        hasher = hashlib.md5()
        f = open(self.localpath, "rb")
        while chunk := f.read(4096):
            hasher.update(chunk)

        self.md5hash = hasher.hexdigest()

        # upload file to s3
        self.s3stash()

    def stash_remote(self):
        # replicate hash_cache? right now this is stored in redis
        # it allows us to be polite and not re-fetch files if we have them already
        self.validate_url()
        self.set_request_session()
        fetch_request = self.build_fetch_request()
        response = self.http.get(**fetch_request)
        response.raise_for_status()

        # if md5 in hash-cache and 304 Not Modified since last time fetched, return md5hash

        # skip if Content-Type header isn't for an image?
        #self.mime_type = response.headers['Content-Type']

        # get values from headers for hash_cache:
        # set 'If-None-Match' = 'ETag' in cache
        # set 'If-Modified-Since' = 'Last-Modified' in cache

        # fetch the file to /tmp and get md5hash
        hasher = hashlib.md5()
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        self.localpath = tmpfile.name
        with tmpfile as f:
            for block in response.iter_content(chunk_size=None):
                hasher.update(block)
                f.write(block)
        self.md5hash = hasher.hexdigest()

        self.get_file_info()

        if not self.is_image:
            print(f"File is not of type image. Not stashing: {self.url}")
            return

        # if the md5hash is in the hash_cache, delete tempfile and return

        # upload file to s3
        self.s3stash()

        os.remove(tmpfile.name)

        # update hash-cache with s3_url, mime-type, dimensions
        # these values must get written to solr at some point?

    def build_fetch_request(self):
        request = {
            'url': self.url,
            'stream': True,
            'timeout': (TIMEOUT_CONNECT, TIMEOUT_READ)
        }

        if self.basic_auth:
            auth = (BASIC_USER, BASIC_PASS)
            request['auth'] = auth

        # add 'If-None-Match' header based on hash_cache
        # add 'If-Modified-Since' header based on hash_cache

        return request

    def s3stash(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#multipart-transfers
        # raise error if not self.md5hash, self.mime_type, self.localpath?

        self.s3key = f"{S3_THUMBNAIL_FOLDER}/{self.md5hash}"
        if not self.already_stashed():
            # Set the desired multipart threshold value (5GB)
            GB = 1024 ** 3
            config = TransferConfig(multipart_threshold=5*GB)
            extra_args = {'ContentType': self.mime_type}

            # Perform the transfer
            self.s3.upload_file(self.localpath, S3_THUMBNAIL_BUCKET, self.s3key, ExtraArgs=extra_args, Config=config)
            print(f"stashed on s3: s3://{S3_THUMBNAIL_BUCKET}/{self.s3key}")

    def validate_url(self):
        parsed = urlparse(self.url)
        try:
            (scheme, netloc, path) = (parsed.scheme, parsed.netloc, parsed.path)
        except:
            print(f"Invalid URL! {self.url}")
            sys.exit()

        if self.basic_auth and scheme != 'https':
            print(f"Basic Auth not over HTTPS is a bad idea!: {self.url}")
            sys.exit()

    def set_request_session(self):
        retry_strategy = Retry(
            total=3,
            status_forcelist=[413, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    def already_stashed(self):
        try:
            response = self.s3.head_object(
                Bucket=S3_THUMBNAIL_BUCKET,
                Key=self.s3key
            )
            print(f"already stashed: s3://{S3_THUMBNAIL_BUCKET}/{self.s3key}")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

    def get_file_info(self):
        try:
            self.dimensions = Image.open(self.localpath).size
        except UnidentifiedImageError:
            self.dimensions = (0,0)
            self.is_image = False
            self.mime_type = None
        else:
            self.is_image = True
            self.mime_type = magic.Magic(mime=True).from_file(self.localpath)



