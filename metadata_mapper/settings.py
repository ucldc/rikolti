import os

from dotenv import load_dotenv

load_dotenv()

LOCAL_RUN = os.environ.get('MAPPER_LOCAL_RUN', False)
DATA_SRC = os.environ.get('MAPPER_DATA_SRC', 's3')
DATA_DEST = os.environ.get('MAPPER_DATA_DEST', 's3')
SKIP_UNDEFINED_ENRICHMENTS = os.environ.get('SKIP_UNDEFINED_ENRICHMENTS', False)

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)

if not LOCAL_RUN and (DATA_SRC == 'local' or DATA_DEST == 'local'):
    print(
        "A local data source or local data destination "
        "is only valid when the application is run locally"
    )
    exit()


def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir,
        'rikolti_bucket',
        folder,
        str(collection_id),
    ])
    return local_path
