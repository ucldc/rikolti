import os

from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', False)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', False)
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', False)
AWS_REGION = os.environ.get('AWS_REGION', False)

NUXEO_USER = os.environ.get('NUXEO_USER', '')
NUXEO_PASS = os.environ.get('NUXEO_PASS', '')

CONTENT_PROCESSES = {
    'magick': '/usr/bin/convert',
    'tiff2rgba': '/usr/bin/tiff2rgba',
    'ffmpeg': '/usr/bin/ffmpeg',
    'ffprobe': '/usr/bin/ffprobe',
}
