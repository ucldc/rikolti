import os

from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

CONTENT_ROOT_URL = os.environ.get("CONTENT_ROOT", 'file:///tmp')
CONTENT_ROOT = {
    "STORE": urlparse(CONTENT_ROOT_URL).scheme,
    "BUCKET": urlparse(CONTENT_ROOT_URL).netloc,
    "PATH": urlparse(CONTENT_ROOT_URL).path,
}

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', False)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', False)
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', False)
AWS_REGION = os.environ.get('AWS_REGION', False)

NUXEO_USER = os.environ.get('NUXEO_USER', False)
NUXEO_PASS = os.environ.get('NUXEO_PASS', False)

CONTENT_PROCESSES = {
    'magick': '/usr/bin/convert',
    'tiff2rgba': '/usr/bin/tiff2rgba',
    'ffmpeg': '/usr/bin/ffmpeg',
    'ffprobe': '/usr/bin/ffprobe',
}
