import logging
import os

from urllib.parse import urlparse

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')

DATA_DEST_URL = os.environ.get("FETCHER_DATA_DEST", "file:///tmp/")
DATA_DEST = {
    "STORE": urlparse(DATA_DEST_URL).scheme,
    "BUCKET": urlparse(DATA_DEST_URL).netloc,
    "PATH": urlparse(DATA_DEST_URL).path
}

for key, value in os.environ.items():
    logger.debug(f"{key}={value}")
