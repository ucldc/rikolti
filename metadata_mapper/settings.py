import os

from dotenv import load_dotenv

load_dotenv()

SKIP_UNDEFINED_ENRICHMENTS = os.environ.get('SKIP_UNDEFINED_ENRICHMENTS', None)

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', None)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', None)
