import os
from file_fetchers.fetcher import Fetcher
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

#S3_PRIVATE_BUCKET = os.environ['S3_PRIVATE_BUCKET']
S3_PRIVATE_BUCKET = 'barbarahui_test_bucket'
S3_MEDIA_INSTRUCTIONS_FOLDER = os.environ['S3_MEDIA_INSTRUCTIONS_FOLDER']
NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']

class NuxeoFetcher(Fetcher):
    def __init__(self, collection_id, **kwargs):
        super(NuxeoFetcher, self).__init__(collection_id, **kwargs)

    def fetch_files(self):
        """ Fetch and stash all files needed by Calisphere for a Nuxeo object
            For Nuxeo objects, this is based on the media_instructions.jsonl for the object
            Return updated media_instructions.jsonl
        """
        for s3key in self.build_instruction_list():
            instructions = self.fetch_instructions(s3key)

            # Validate instructions. What values must be provided?

            # preprocess parent component
            instructions['contentFile'] = self.preprocess_content_file(instructions)
            instructions['thumbnail'] = self.preprocess_thumbnail(instructions)

            # preprocess child components
            for child in instructions['children']:
                child['contentFile'] = self.preprocess_content_file(child)
                child['thumbnail'] = self.preprocess_thumbnail(child)
            
            # set object thumbnail (this will go into the ElasticSearch index)
            if not instructions.get('objectThumbnail'):
                instructions['objectThumbnail'] = {}
            instructions['objectThumbnail']['url'] = self.get_object_thumbnail_url(instructions)

            # stash parent files
            instructions['contentFile']['s3_uri'] = self.stash_content_file(instructions)
            instructions['thumbnail']['md5hash'] = self.stash_thumbnail(instructions)

            # stash child files
            for child in instructions['children']:
                child['contentFile']['s3_uri'] = self.stash_content_file(child)
                child['thumbnail']['md5hash'] = self.stash_thumbnail(child)

            # stash object thumbnail
            instructions['objectThumbnail']['md5hash'] = self.stash_thumbnail(instructions)

            self.update_instructions(s3key, instructions)

            # return a json representation of stashed files

    def preprocess_content_file(self, instructions):
        if instructions['contentFile']['mime-type'].startswith('image/'):
            instructions['contentFile'] = self.create_jp2(instructions)

        if instructions['contentFile']['mime-type'] == 'application/pdf':
            instructions['thumbnail'] = self.create_pdf_thumb(instructions)

        return instructions['contentFile']

    def preprocess_thumbnail(self, instructions):
        if not instructions['thumbnail']:
            instructions['thumbnail'] = {}
        return instructions['thumbnail']

    def create_jp2(self, instructions):
        # recreate this using openJPEG instead of Kakadu: 
        # https://github.com/barbarahui/ucldc-iiif/blob/master/ucldc_iiif/convert.py
        return instructions['contentFile']

    def create_pdf_thumbnail(self, instructions):
        # download pdf
        '''
        fetch_request = self.build_fetch_request(instructions)
        response = self.http.get(**fetch_request)
        response.raise_for_status()

        tmpfile = ''
        with open(tmpfile, 'wb') as f:
            for block in response.iter_content(chunk_size=None):
                f.write(block)
        '''

        # create jpg of first page
        # see: https://github.com/barbarahui/nuxeo-calisphere/blob/master/s3stash/nxstash_thumb.py#L158-L190

        # stash on s3
        # update instructions['thumbnail']
        return instructions['thumbnail']

    def build_fetch_request(self, instructions):
        # https://github.com/barbarahui/nuxeo-calisphere/blob/master/s3stash/nxstashref.py#L78-L106
        # timeouts based on those used by nuxeo-python-client
        # see: https://github.com/nuxeo/nuxeo-python-client/blob/master/nuxeo/constants.py
        # but tweaked to be slightly larger than a multiple of 3, which is recommended
        # in the requests documentation.
        # see: https://docs.python-requests.org/en/master/user/advanced/#timeouts
        timeout_connect = 12.05
        timeout_read = (60 * 10) + 0.05
        source_url = instructions['contentFile']['url']
        request = {
            'url': source_url,
            'auth': (NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
            'stream': True,
            'timeout': (timeout_connect, timeout_read)
        }
        return request

    def build_instruction_list(self):
        prefix = f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}"

        response = self.s3.list_objects_v2(
            Bucket=S3_PRIVATE_BUCKET,
            Prefix=prefix
        )

        return ['media_instructions/76/uid=a0df3f41-8c54-42dd-b7f3-f3bf95011c9f/a0df3f41-8c54-42dd-b7f3-f3bf95011c9f.jsonl']
        return [content['Key'] for content in response['Contents']]

    def fetch_instructions(self, s3key):
        response = self.s3.get_object(
            Bucket=S3_PRIVATE_BUCKET,
            Key=s3key
        )
        instructions = json.loads(response['Body'].read())

        return instructions

    def stash_content_file(self, instructions):
        """ stash content file(s) for one object on s3 """
        if instructions.get('calisphere-id'):
            identifier = instructions['calisphere-id']
        elif instructions.get('id'):
            identifier = instructions['id']
        else:
            print("raise an error! no ID")

        filename = instructions['contentFile']['filename']

        fetch_request = self.build_fetch_request(instructions)

        return Fetcher.stash_content_file(self, identifier, filename, fetch_request)

    def stash_thumbnail(self, instructions):
        # run md5s3stash 
        # https://github.com/ucldc/md5s3stash
        # 
        # if source copy is the pdf thumbnail just created, we can now delete the source copy from s3
        return "<md5hash>"

    def get_object_thumbnail_url(self, instructions):
        # recreate this logic: https://github.com/ucldc/harvester/blob/master/harvester/fetcher/nuxeo_fetcher.py#L79-L141
        url = "<source_url_for_object_thumbnail>"
        return url

    def update_instructions(self, s3key, instructions):
        print(f"update on s3: {s3key}\n")
        #pp.pprint(instructions)

