import os

from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

DATA_SRC_URL = os.environ.get('MAPPER_DATA_SRC', 'file:///tmp')
DATA_SRC = {
    "STORE": urlparse(DATA_SRC_URL).scheme,
    "BUCKET": urlparse(DATA_SRC_URL).netloc,
    "PATH": urlparse(DATA_SRC_URL).path
}

DATA_DEST_URL = os.environ.get('MAPPER_DATA_DEST', 'file:///tmp')
DATA_DEST = {
    "STORE": urlparse(DATA_DEST_URL).scheme,
    "BUCKET": urlparse(DATA_DEST_URL).netloc,
    "PATH": urlparse(DATA_DEST_URL).path
}

SKIP_UNDEFINED_ENRICHMENTS = os.environ.get('SKIP_UNDEFINED_ENRICHMENTS', False)

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)

def local_path(folder, collection_id):
    local_path = os.sep.join([
        DATA_SRC["PATH"],
        folder,
        str(collection_id),
    ])
    return local_path
