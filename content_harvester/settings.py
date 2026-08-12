import os

from dotenv import load_dotenv

load_dotenv()

AWS_CREDENTIALS = {
    "aws_access_key_id": os.environ.get('AWS_ACCESS_KEY_ID', None),
    "aws_secret_access_key": os.environ.get('AWS_SECRET_ACCESS_KEY', None),
    "aws_session_token": os.environ.get('AWS_SESSION_TOKEN', None),
    "aws_region": os.environ.get('AWS_REGION', None)
}

NUXEO_USER = os.environ.get('NUXEO_USER', '')
NUXEO_PASS = os.environ.get('NUXEO_PASS', '')

CONTENT_PROCESSES = {
    'magick': '/usr/bin/convert',
    'tiff2rgba': '/usr/bin/tiff2rgba',
    'ffmpeg': '/usr/bin/ffmpeg',
    'ffprobe': '/usr/bin/ffprobe',
}
