import os

from dotenv import load_dotenv

load_dotenv()

DATA_SRC = os.environ.get('INDEXER_DATA_SRC', 's3')
S3_BUCKET = os.environ.get('S3_BUCKET', False)

ENDPOINT = os.environ.get('RIKOLTI_ES_ENDPOINT')
AUTH = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', False)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', False)
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', False)
AWS_REGION = os.environ.get('AWS_REGION', False)

def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir,
        'rikolti_data',
        folder,
        str(collection_id),
    ])
    return local_path