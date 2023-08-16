import os
import sys
import logging
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

load_dotenv()

DATA_DEST = os.environ.get('FETCHER_DATA_DEST', 's3')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')

for key, value in os.environ.items():
    logger.debug(f"{key}={value}")
