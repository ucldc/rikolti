import os

from dotenv import load_dotenv

load_dotenv()

LOCAL_RUN = True
DATA_DEST = 'local'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
NUXEO_TOKEN = os.environ.get('NUXEO')
FLICKR_API_KEY = os.environ.get('FLICKR_API_KEY')

DATA_SRC = 'local'
SKIP_UNDEFINED_ENRICHMENTS = False

SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)

#S3_BUCKET = os.environ.get('S3_BUCKET', False)

if not LOCAL_RUN and DATA_DEST == 'local':
    print(
        "A local data destination is only valid "
        "when the application is run locally"
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
