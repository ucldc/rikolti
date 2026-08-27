import logging
import os
import sys

from dotenv import load_dotenv

try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)

load_dotenv()

NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')
CALISPHERE_ETL_TOKEN = os.environ.get('CALISPHERE_ETL_TOKEN')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')

if AIRFLOW_AVAILABLE:
    PRESERVICA_AUTH = Variable.get(
        'rikolti_preservica_auth', deserialize_json=True, default_var={})
else:
    PRESERVICA_AUTH = {
        "username": os.environ.get('PRESERVICA_USER'),
        "password": os.environ.get('PRESERVICA_PASS')
    }

# print all constants defined in settings, not just the os env vars:
for key, value in os.environ.items():
    logger.debug(f"{key}={value}")
# current_module = sys.modules[__name__]
# for key, value in current_module.__dict__.items():
#     if key.isupper():
#         logger.debug(f"{key}={value}")
