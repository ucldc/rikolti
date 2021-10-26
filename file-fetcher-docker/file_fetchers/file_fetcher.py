import os
import boto3
import json
import requests

""" fetch file(s) for a given object. Or should this be for a collection?
    ultimately, we want flexibility. Usually will kick it off for a collection,
    but sometimes we will want the option to fun for just one object.
    
    what should this parent class do? 
    - take a metadata record - in the case of nuxeo, is this the media.json?
    - figure out the source url??????
    - stash content file on s3

    return: {
    ‘calisphere-id’: <nuxeo id, calisphere id>, 
    ‘href’: <original URL>,
    ‘format’: <format>
}


"""

S3_PUBLIC_BUCKET = os.environ['S3_PUBLIC_BUCKET']
S3_PRIVATE_BUCKET = os.environ['S3_PRIVATE_BUCKET']
S3_CONTENT_FILES_FOLDER = os.environ['S3_CONTENT_FILES_FOLDER']
S3_MEDIA_INSTRUCTIONS_FOLDER = os.environ['S3_MEDIA_INSTRUCTIONS_FOLDER']

class FileFetcher(object):
    def __init__(self, collection_id, fetcher_type):
        self.collection_id = collection_id
        self.harvest_type = fetcher_type
        self.s3 = boto3.client('s3')
        # check for empty list

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
        source_url = media_instructions['contentFile']['url']
        mimetype = media_instructions['contentFile']['mime-type']

        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{calisphere_id}::{filename}"
        print(f"{s3_key=}")


        # check to see if key already exists on s3
        # TODO: allow for clean restash
        try:
            response = self.s3.head_object(
                Bucket=S3_PUBLIC_BUCKET,
                Key=s3_key
            )
            s3_key_exists = True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                s3_key_exists = False
                print(f"stashing {s3_key}")

        if s3_key_exists is False:
            # download file
            requests_session = requests.Session()
            response = requests_session.get(source_url,
                auth=(NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
                stream=True)
            response.raise_for_status()

            # upload file
            with response as part:

                # stash content file on s3
                # https://amalgjose.com/2020/08/13/python-program-to-stream-data-from-a-url-and-write-it-to-s3/
                # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html

                part.raw.decode_content = True
                if s3_key_exists is False:
                    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
                    self.s3.upload_fileobj(part.raw, S3_PUBLIC_BUCKET, s3_key, Config=conf)

                # stash thumbnail on s3
                if mimetype == 'application/pdf':
                    print(f"create thumbnail and stash on s3 at {s3_key=}")
                    # download copy of contentfile to /tmp
                    '''
                    local_filepath = f"/tmp/{filename}"

                    with open(local_filepath, 'wb') as f:
                        for block in part.iter_content(chunk_size=None):
                            f.write(block)

                    # create thumbnail
                    self.create_pdf_thumbnail(local_filepath, calisphere_id)

                    # updoad thumbnail to s3
                    s3_key = f"nuxeo-thumbnails/{calisphere_id}::thumbnail.png"

                    # delete local files
                    '''
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

    def build_fetch_request(self):
        """build parameters for the institution's http_client.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientSession.get
        """
        pass

    def json(self):
        pass

