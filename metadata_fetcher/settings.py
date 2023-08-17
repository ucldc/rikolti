import logging
import os
import sys

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

LOCAL_RUN = os.environ.get('FETCHER_LOCAL_RUN', False)
DATA_DEST = os.environ.get('FETCHER_DATA_DEST', 's3')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')

if not LOCAL_RUN and DATA_DEST == 'local':
    print(
        "A local data destination is only valid "
        "when the application is run locally",
        file=sys.stderr
    )
    exit()

for key, value in os.environ.items():
    logger.debug(f"{key}={value}")
