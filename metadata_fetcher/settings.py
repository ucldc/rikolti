import logging
import os

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')
CALISPHERE_ETL_TOKEN = os.environ.get('CALISPHERE_ETL_TOKEN')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
PRESERVICA_USER = os.environ.get('PRESERVICA_USER')
PRESERVICA_PASS = os.environ.get('PRESERVICA_PASS')

for key, value in os.environ.items():
    logger.debug(f"{key}={value}")
