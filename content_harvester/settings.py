import os

from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

DATA_SRC_URL = os.environ.get('CONTENT_DATA_SRC', 'file:///tmp')
DATA_SRC = {
    "STORE": urlparse(DATA_SRC_URL).scheme,
    "BUCKET": urlparse(DATA_SRC_URL).netloc,
    "PATH": urlparse(DATA_SRC_URL).path
}

DATA_DEST_URL = os.environ.get('CONTENT_DATA_DEST', 'file:///tmp')
DATA_DEST = {
    "STORE": urlparse(DATA_DEST_URL).scheme,
    "BUCKET": urlparse(DATA_DEST_URL).netloc,
    "PATH": urlparse(DATA_DEST_URL).path
}

CONTENT_DEST_URL = os.environ.get("CONTENT_DEST", 'file:///tmp')
CONTENT_DEST = {
    "STORE": urlparse(CONTENT_DEST_URL).scheme,
    "BUCKET": urlparse(CONTENT_DEST_URL).netloc,
    "PATH": urlparse(CONTENT_DEST_URL).path
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

def local_path(collection_id, folder):
    local_path = os.sep.join([
        DATA_SRC["PATH"],
        str(collection_id),
        folder,
    ])
    return local_path
