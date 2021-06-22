import os
import boto3
import requests
import json
from botocore.exceptions import ClientError
import subprocess

NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']
magick_convert_location = os.environ.get('PATH_MAGICK_CONVERT', '/usr/bin/convert')
ffmpeg_location = os.environ.get('PATH_FFMPEG', '/usr/bin/ffmpeg')
ffprobe_location = os.environ.get('PATH_FFPROBE','/usr/bin/ffprobe')

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
        # TODO: allow for clean restash
        try:
            response = s3.head_object(
                Bucket=s3_bucket,
                Key=s3_key
            )
            s3_key_exists = True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                s3_key_exists = False
                print(f"stashing {s3_key}")

        if 1 == 1:
        #if s3_key_exists is False:

            response = requests_session.get(source_url,
                auth=(NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
                stream=True)
            response.raise_for_status()


            with response as part:

                ''' stash content file on s3
                    https://amalgjose.com/2020/08/13/python-program-to-stream-data-from-a-url-and-write-it-to-s3/
                    https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html

                '''
                part.raw.decode_content = True
                if s3_key_exists is False:
                    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                    s3.upload_fileobj(part.raw, s3_bucket, s3_key, Config=conf)

                # stash thumbnail on s3
                if mimetype == 'application/pdf':
                    print(f"create thumbnail and stash on s3 at {s3_key=}")
                    # download copy of contentfile to /tmp
                    local_filepath = f"/tmp/{filename}"

                    with open(local_filepath, 'wb') as f:
                        for block in part.iter_content(chunk_size=None):
                            f.write(block)

                    # create thumbnail
                    self.create_pdf_thumbnail(local_filepath, calisphere_id)

                    # updoad thumbnail to s3
                    s3_key = f"nuxeo-thumbnails/{calisphere_id}::thumbnail.png"


                    # delete local files

                elif mimetype.startswith('video'):
                    s3_key = f"nuxeo-thumbnails/{calisphere_id}::thumbnail.png"
                    print(f"create thumbnail and stash on s3 at {s3_key=}")
                     # TODO: create and stash thumbnail
                elif mimetype.startswith('image'):
                    s3_key = f"nuxeo-high-res-images/{calisphere_id}::{filename}.jp2"
                    print(f"create jp2000 and stash on s3 at {s3_key=}")
                    # TODO: create and stash jp2

            # update media.json with s3_uri?

        else:
            print(f"{s3_key} already exists on s3")


    def create_pdf_thumbnail(self, input_path, calisphere_id):
        """ stash thumbnail image of PDF on s3 """
        output_path = f"{input_path}-thumb.png"
        input_string = f"{input_path}[0]"  # [0] to specify first page of PDF

        result = subprocess.run([magick_convert_location, '-quiet', '-density', '300', input_string, '-quality', '90', output_path])
        # TODO: deal with exceptions

    def stash_large_format_image(self):
        """ stash jp2 image for display in frontend """
        pass
