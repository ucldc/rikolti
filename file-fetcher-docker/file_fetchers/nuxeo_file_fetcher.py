import os
from file_fetchers.file_fetcher import FileFetcher
import subprocess

NUXEO_BASIC_USER = os.environ['NUXEO_BASIC_USER']
NUXEO_BASIC_AUTH = os.environ['NUXEO_BASIC_AUTH']
magick_convert_location = os.environ.get('PATH_MAGICK_CONVERT', '/usr/bin/convert')
ffmpeg_location = os.environ.get('PATH_FFMPEG', '/usr/bin/ffmpeg')
ffprobe_location = os.environ.get('PATH_FFPROBE','/usr/bin/ffprobe')

class NuxeoFileFetcher(FileFetcher):
    def __init__(self, collection_id, fetcher_type, clean):
        super(NuxeoFileFetcher, self).__init__(collection_id, fetcher_type, clean)

    def build_fetch_request(self, media_instructions):
        source_url = media_instructions['contentFile']['url']
        request = {
            'url': source_url,
            'auth': (NUXEO_BASIC_USER, NUXEO_BASIC_AUTH),
            'stream': True
        }
        return request

    def stash_large_format_image(self):
        """ stash jp2 image for display in frontend """
        pass
