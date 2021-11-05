import os
from file_fetchers.fetcher import Fetcher
import json
import tempfile
import subprocess
import boto3
from botocore.exceptions import ClientError
import pprint
pp = pprint.PrettyPrinter(indent=4)

#S3_PUBLIC_BUCKET = os.environ['S3_PUBLIC_BUCKET']
S3_PUBLIC_BUCKET = 'barbarahui_test_bucket'
#S3_PRIVATE_BUCKET = os.environ['S3_PRIVATE_BUCKET']
S3_PRIVATE_BUCKET = 'barbarahui_test_bucket'
S3_MEDIA_INSTRUCTIONS_FOLDER = os.environ['S3_MEDIA_INSTRUCTIONS_FOLDER']
S3_CONTENT_FILES_FOLDER = os.environ['S3_CONTENT_FILES_FOLDER']
NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']
MAGICK_CONVERT = os.environ.get('MAGICK_CONVERT', '/usr/bin/convert')

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

            # stash parent files
            if instructions.get('contentFile'):
                instructions['contentFile'] = self.stash_content_file(instructions)
                instructions['thumbnail'] = self.stash_thumbnail(instructions)

            # stash child files
            for child in instructions['children']:
                if child.get('contentFile'):
                    child['contentFile'] = self.stash_content_file(child)
                    child['thumbnail'] = self.stash_thumbnail(child)

            # stash object thumbnail
            instructions['objectThumbnail'] = self.set_object_thumbnail(instructions)
            if instructions['objectThumbnail'].get('url'):
                instructions['objectThumbnail']['md5hash'] = Fetcher.stash_thumbnail(instructions['objectThumbnail']['url'])

            # stash media.json
            self.stash_media_json(s3key, instructions)

            # return a json representation of stashed files

    def stash_content_file(self, instructions):
        if instructions['contentFile']['mime-type'].startswith('image/'):
            instructions['contentFile'] = self.stash_jp2(instructions)
        else:
            identifier = instructions['calisphere-id']
            filename = instructions['contentFile']['filename']
            fetch_request = self.build_fetch_request(instructions)
            instructions['contentFile']['s3_uri'] = Fetcher.stash_content_file(self, identifier, filename, fetch_request)

        return instructions['contentFile']

    def stash_thumbnail(self, instructions):
        if instructions['contentFile']['mime-type'] == 'application/pdf':
            instructions['thumbnail'] = self.stash_pdf_thumbnail(instructions)
        else:
            instructions['thumbnail']['md5hash'] = Fetcher.stash_thumbnail(instructions['thumbnail']['url'])

        return instructions['thumbnail']

    def stash_jp2(self, instructions):
        """ create a jp2 copy of the image and stash it on s3 """
        basename_no_ext = self.get_basename_no_ext(instructions['contentFile']['filename'])
        filename_jp2 = f"{basename_no_ext}.jp2"
        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{instructions['calisphere-id']}::{filename_jp2}"

        if self.clean_stash or not self.already_stashed(S3_PUBLIC_BUCKET, s3_key):
            source_fullpath = self.fetch_to_temp(instructions)
            jp2_fullpath = f"{tempfile.gettempdir()}/{filename_jp2}"

            # create jp2 copy of source image
            rate = 10 # factor of compression. 20 means 20 times compressed.
            args = [MAGICK_CONVERT, source_fullpath, "-format", "-jp2", "-define", f"jp2:rate={rate}",jp2_fullpath]
            subprocess.run(args, check=True)

            self.s3.upload_file(jp2_fullpath, S3_PUBLIC_BUCKET, s3_key)
            print(f"stashed on s3: s3://{S3_PUBLIC_BUCKET}/{s3_key}")

            os.remove(source_fullpath)
            os.remove(jp2_fullpath)

        instructions['contentFile'].update({
            "filename": filename_jp2,
            "mime-type": "image/jp2",
            "url": f"https://s3.amazonaws.com/{S3_PUBLIC_BUCKET}/{s3_key}",
            "s3_uri": f"s3://{S3_PUBLIC_BUCKET}/{s3_key}"
        })

        return instructions['contentFile']

    def stash_pdf_thumbnail(self, instructions):
        basename_no_ext = self.get_basename_no_ext(instructions['contentFile']['filename'])
        filename_png = f"first_page-{basename_no_ext}.png"
        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{instructions['calisphere-id']}::{filename_png}"

        if self.clean_stash or not self.already_stashed(S3_PUBLIC_BUCKET, s3_key):
            pdf_fullpath = self.fetch_to_temp(instructions)
            png_fullpath = f"{tempfile.gettempdir()}/{filename_png}"

            # create png of first page of PDF
            args = [MAGICK_CONVERT, "-strip", "-format", "png", "-quality", "75", f"{pdf_fullpath}[0]", png_fullpath]
            subprocess.run(args, check=True)

            # FIXME stash using md5s3stash instead!!
            self.s3.upload_file(png_fullpath, S3_PUBLIC_BUCKET, s3_key)
            print(f"stashed on s3: s3://{S3_PUBLIC_BUCKET}/{s3_key}")

            os.remove(pdf_fullpath)
            os.remove(png_fullpath)

        instructions['thumbnail'] = {
            "filename": filename_png,
            "mime-type": "image/png",
            "url": f"https://s3.amazonaws.com/{S3_PUBLIC_BUCKET}/{s3_key}",
            "md5hash": "<md5hash>"
        }

        return instructions['thumbnail']

    def get_basename_no_ext(self, filename):
        return f"{os.path.splitext(os.path.basename(filename))[0]}"

    def fetch_to_temp(self, instructions):
        # download file
        fetch_request = self.build_fetch_request(instructions)
        response = self.http.get(**fetch_request)
        response.raise_for_status()

        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        with tmpfile as f:
            for block in response.iter_content(chunk_size=None):
                f.write(block)

        return tmpfile.name

    def build_fetch_request(self, instructions):
        """
        timeouts based on those used by nuxeo-python-client
        see: https://github.com/nuxeo/nuxeo-python-client/blob/master/nuxeo/constants.py
        but tweaked to be slightly larger than a multiple of 3, which is recommended
        in the requests documentation.
        see: https://docs.python-requests.org/en/master/user/advanced/#timeouts
        """
        timeout_connect = 12.05
        timeout_read = (60 * 10) + 0.05
        source_url = instructions['contentFile']['url']
        source_url = source_url.replace('/nuxeo/', '/Nuxeo/')
        request = {
            'url': source_url,
            'auth': (NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
            'stream': True,
            'timeout': (timeout_connect, timeout_read)
        }
        return request

    def build_instruction_list(self):
        prefix = f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}/"

        response = self.s3.list_objects_v2(
            Bucket=S3_PRIVATE_BUCKET,
            Prefix=prefix
        )

        # FIXME figure out how to only return the objects within the folder, not the folder itself
        return [content['Key'] for content in response['Contents'] if content['Key'] != f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}/"]

    def fetch_instructions(self, s3key):
        response = self.s3.get_object(
            Bucket=S3_PRIVATE_BUCKET,
            Key=s3key
        )
        instructions = json.loads(response['Body'].read())

        return instructions

    def set_object_thumbnail(self, instructions):
        # try to find a thumbnail url for an image type component first
        # check parent
        url = self.get_thumb_url_image_source_only(instructions)
        if url:
            return {'url': url}

        # check children
        for child in instructions['children']:
            url = self.get_thumb_url_image_source_only(child)
            if url:
                return {'url': url}

        # failing that, find any thumbnail url
        # check parent
        url = self.get_thumb_url(instructions)
        if url:
            return {'url': url}

        # check children
        for child in instructions['children']:
            url = self.get_thumb_url(child)
            return {'url': url}

        return {'url': None}

    def get_thumb_url_image_source_only(self, instructions):
        ''' if component json contains url suitable as input for thumbnailer, return it
            the source content file must be of type image
        '''
        url = None
        try:
            if instructions['contentFile']['mime-type'].startswith('image/'):
                url = instructions['thumbnail']['url']
        except KeyError:
            pass

        return url


    def get_thumb_url(self, instructions):
        ''' if component json contains url suitable as input for thumbnailer, return it '''
        url = None
        try:
            url = instructions['thumbnail']['url']
        except KeyError:
            pass

        return url

    def stash_media_json(self, s3key, instructions):
        path = os.path.join(os.getcwd(), f"{self.collection_id}")
        if not os.path.exists(path):
            os.mkdir(path)

        mediajson_path = os.path.join(path, "media_json")
        if not os.path.exists(mediajson_path):
            os.mkdir(mediajson_path)

        filename = os.path.join(mediajson_path, f"{instructions['calisphere-id']}")
        f = open(filename, "w")
        media_json = json.dumps(instructions)
        f.write(media_json)