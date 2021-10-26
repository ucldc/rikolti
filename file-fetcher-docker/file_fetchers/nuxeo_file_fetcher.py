import os
import subprocess
import requests
import boto3
import json
from botocore.exceptions import ClientError
from file_fetchers.file_fetcher import FileFetcher

NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']
magick_convert_location = os.environ.get('PATH_MAGICK_CONVERT', '/usr/bin/convert')
ffmpeg_location = os.environ.get('PATH_FFMPEG', '/usr/bin/ffmpeg')
ffprobe_location = os.environ.get('PATH_FFPROBE','/usr/bin/ffprobe')

class NuxeoFileFetcher(FileFetcher):
    def __init__(self, collection_id, fetcher_type):
        super(NuxeoFileFetcher, self).__init__(collection_id, fetcher_type)

    def build_fetch_request(self):
        url = ''
        headers = ''
        params = {}
        request = {'url': url, 'headers': headers, 'params': params}
        return request

    def create_pdf_thumbnail(self, input_path, calisphere_id):
        """ stash thumbnail image of PDF on s3 """
        output_path = f"{input_path}-thumb.png"
        input_string = f"{input_path}[0]"  # [0] to specify first page of PDF

        result = subprocess.run([magick_convert_location, '-quiet', '-density', '300', input_string, '-quality', '90', output_path])
        # TODO: deal with exceptions

    def stash_large_format_image(self):
        """ stash jp2 image for display in frontend """
        pass
