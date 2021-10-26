import os
import boto3
import json
import requests

S3_PUBLIC_BUCKET = os.environ['S3_PUBLIC_BUCKET']
S3_PRIVATE_BUCKET = os.environ['S3_PRIVATE_BUCKET']
S3_CONTENT_FILES_FOLDER = os.environ['S3_CONTENT_FILES_FOLDER']
S3_MEDIA_INSTRUCTIONS_FOLDER = os.environ['S3_MEDIA_INSTRUCTIONS_FOLDER']

class FileFetcher(object):
    def __init__(self, collection_id, fetcher_type, clean):
        self.collection_id = collection_id
        self.harvest_type = fetcher_type
        self.clean_stash = clean

        self.s3 = boto3.client('s3')

    def fetch_content_files(self):
        """Fetch content files to store in s3

        Fetch media instruction file(s) from s3 - records stored in json line format
        Fetch content file(s) from Nuxeo and stash into s3
        """
        for instruction_key in self.build_media_instruction_list():
            media_instructions = self.fetch_media_instructions(instruction_key)
            self.stash_content_files(media_instructions)

    def build_media_instruction_list(self):
        prefix = f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}"

        response = self.s3.list_objects_v2(
            Bucket=S3_PRIVATE_BUCKET,
            Prefix=prefix
        )

        return [content['Key'] for content in response['Contents']]

    def fetch_media_instructions(self, s3key):
        response = self.s3.get_object(
            Bucket=S3_PRIVATE_BUCKET,
            Key=s3key
        )
        media_instructions = json.loads(response['Body'].read())

        return media_instructions

    def stash_content_files(self, media_instructions):
        """ stash content files for one object on s3 """
        calisphere_id = media_instructions['calisphere-id']
        filename = media_instructions['contentFile']['filename']

        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{calisphere_id}::{filename}"
        print(f"{s3_key=}")

        # check to see if key already exists on s3
        if self.clean_stash:
            do_stash = True
        else:
            try:
                response = self.s3.head_object(
                    Bucket=S3_PUBLIC_BUCKET,
                    Key=s3_key
                )
                do_stash = False
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    do_stash = True

        if do_stash:
            # download file
            fetch_request = self.build_fetch_request(media_instructions)
            response = requests.get(**fetch_request)
            response.raise_for_status()

            # stash file on s3
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html
            with response as part:
                part.raw.decode_content = True
                conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                self.s3.upload_fileobj(part.raw, S3_PUBLIC_BUCKET, s3_key, Config=conf)
        else:
            print(f"{s3_key} not stashing")

    def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        requests.get()
        """
        raise NotImplementedError

