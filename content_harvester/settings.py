import os

from dotenv import load_dotenv

load_dotenv()

METADATA_SRC = 'local'
METADATA_DEST = 's3'
CONTENT_DEST = 's3'

S3_BUCKET = os.environ.get('S3_BUCKET', False)
S3_CONTENT_BUCKET = os.environ.get('S3_CONTENT_BUCKET', False)
S3_BASE_URL = os.environ.get('S3_BASE_URL', False)

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', False)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', False)
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', False)
AWS_REGION = os.environ.get('AWS_REGION', False)

NUXEO_USER = os.environ.get('NUXEO_BASIC_USER', False)
NUXEO_PASS = os.environ.get('NUXEO_BASIC_PASS', False)

CONTENT_PROCESSES = {
    'magick': '/usr/bin/convert',
    'tiff2rgba': '/usr/bin/tiff2rgba',
    'ffmpeg': '/usr/bin/ffmpeg',
    'ffprobe': '/usr/bin/ffprobe',
}

def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir,
        'rikolti_bucket',
        folder,
        str(collection_id),
    ])
    return local_path
