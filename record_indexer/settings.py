import json
import os

from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

DATA_SRC_URL = os.environ.get('INDEXER_DATA_SRC', 'file:///tmp/')
DATA_SRC = {
    "STORE": urlparse(DATA_SRC_URL).scheme,
    "BUCKET": urlparse(DATA_SRC_URL).netloc,
    "PATH": urlparse(DATA_SRC_URL).path
}

ENDPOINT = os.environ.get('RIKOLTI_ES_ENDPOINT')
AUTH = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

def local_path(collection_id, folder):
    local_path = os.sep.join([
        DATA_SRC["PATH"],
        str(collection_id),
        folder
    ])
    return local_path

RIKOLTI_HOME = os.environ.get('RIKOLTI_HOME')
RECORD_INDEX_CONFIG = os.sep.join([
        RIKOLTI_HOME,
        'record_indexer/index_templates/record_index_config.json'
    ])