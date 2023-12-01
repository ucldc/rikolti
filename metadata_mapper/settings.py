import os

from dotenv import load_dotenv

load_dotenv()

SKIP_UNDEFINED_ENRICHMENTS = os.environ.get('SKIP_UNDEFINED_ENRICHMENTS', False)

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)
COUCH_URL = os.environ.get('UCLDC_COUCH_URL', False)

COUCH_TIMEOUT = int(os.environ.get('UCLDC_COUCH_TIMEOUT', 60))
