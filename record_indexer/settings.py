import os

from dotenv import load_dotenv

load_dotenv()

ENDPOINT = os.environ.get("RIKOLTI_ES_ENDPOINT")
AUTH = ("rikolti", os.environ.get("RIKOLTI_ES_PASS"))

RIKOLTI_HOME = os.environ.get("RIKOLTI_HOME", "/usr/local/airflow/dags/rikolti")
RECORD_INDEX_CONFIG = os.sep.join(
    [RIKOLTI_HOME, "record_indexer/index_templates/record_index_config.json"]
)

INDEX_RETENTION = os.environ.get("INDEX_RETENTION", 1)
