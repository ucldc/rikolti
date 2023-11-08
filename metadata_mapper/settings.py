import os

from dotenv import load_dotenv

load_dotenv()

DATA_SRC_URL = os.environ.get('MAPPER_DATA_SRC', 'file:///tmp')
DATA_DEST_URL = os.environ.get('MAPPER_DATA_DEST', 'file:///tmp')

SKIP_UNDEFINED_ENRICHMENTS = os.environ.get('SKIP_UNDEFINED_ENRICHMENTS', False)

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)
COUCH_URL = os.environ.get('UCLDC_COUCH_URL', False)

