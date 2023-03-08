import os

from dotenv import load_dotenv

load_dotenv()

LOCAL_RUN = True
DEV = True
DATA_SRC = os.environ.get('CONTENT_FETCHER_DATA_SRC', 's3')
DATA_DEST = os.environ.get('CONTENT_FETCHER_DATA_DEST', 's3')

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
S3_CONTENT_BUCKET = os.environ.get('S3_CONTENT_BUCKET', False)
S3_BASE_URL = os.environ.get('S3_BASE_URL', False)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', False)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', False)
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', False)

NUXEO_TOKEN = os.environ.get('NUXEO', False)
NUXEO_USER = os.environ.get('NUXEO_BASIC_USER', False)
NUXEO_PASS = os.environ.get('NUXEO_BASIC_PASS', False)

CONTENT_PROCESSES = {
    'magick': '/usr/bin/convert',
    'tiff2rgba': '/usr/bin/tiff2rgba',
    'ffmpeg': '/usr/bin/ffmpeg',
    'ffprobe': '/usr/bin/ffprobe',
    # 'kdu_expand': '/usr/local/bin/kdu_expand',
    # 'kdu_compress': '/usr/local/bin/kdu_compress',
}

if not LOCAL_RUN and (DATA_SRC == 'local' or DATA_DEST == 'local'):
    print(
        "A local data source or local data destination "
        "is only valid when the application is run locally"
    )
    exit()


def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir,
        'rikolti_bucket',
        folder,
        str(collection_id),
    ])
    return local_path
