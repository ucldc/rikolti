import os
import boto3
import requests
import json
from botocore.exceptions import ClientError

NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']

class NuxeoFileFetcher(object):

    def __init__(self, collection_id):
        self.collection_id = collection_id
        # TODO: allow for fetching files for single object, or an arbitrary list of objects?

    def fetch_content_files(self):
        """ fetch content files to store in s3 """
        print(f"fetching content files for collection {self.collection_id=}")

        s3_bucket = 'ucldc-ingest'
        s3 = boto3.client('s3')
        prefix = f"glue-test-data-target/mapped/{self.collection_id}-media-2021-06-01"

        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=prefix
        )

        for content in response['Contents']:
            # read in the json stored at Key
            response = s3.get_object(
                Bucket=s3_bucket,
                Key=content['Key']
            )
            media_json = json.loads(response['Body'].read())

            if media_json['contentFile'] is not None:
                self.stash_content_files(media_json)

        return "Success" #TODO: return json representation of something useful

    def stash_content_files(self, media_json):
        """ stash content files on s3 """
        #print(f"{media_json=}")
        calisphere_id = media_json['calisphere-id']
        filename = media_json['contentFile']['filename']
        source_url = media_json['contentFile']['url']
        mimetype = media_json['contentFile']['mime-type']

        s3_bucket = 'ucldc-ingest'
        s3 = boto3.client('s3')
        requests_session = requests.Session()

        s3_key = f"content-files-fetched/{calisphere_id}::{filename}"
        print(f"{s3_key=}")

        # check to see if key already exists on s3
        try:
            response = s3.head_object(
                Bucket=s3_bucket,
                Key=s3_key
            )
            print(f"{s3_key} already exists on s3")

        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(f"stashing {s3_key}")
                # https://amalgjose.com/2020/08/13/python-program-to-stream-data-from-a-url-and-write-it-to-s3/
                response = requests_session.post(source_url,
                    auth=(NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
                    stream=True)
                with response as part:
                    part.raw.decode_content = True
                    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                    s3.upload_fileobj(part.raw, s3_bucket, s3_key, Config=conf)

                # update media.json with s3_uri

        if mimetype == 'application/pdf':
            print(f"{mimtype=} - download file to /tmp so we can create PDF thumbnail")
        elif mimetype.startswith('video'):
             print(f"{mimetype=} - download file to /tmp so we can create a video thumbnail")
        elif mimetype.startswith('image'):
            print(f"{mimetype=} - download file to /tmp so we can create a jp2")


    def stash_large_format_image(self):
        """ stash jp2 image for display in frontend """
        pass
