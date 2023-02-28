import os

from dotenv import load_dotenv

load_dotenv()

LOCAL_RUN = os.environ.get('FETCHER_LOCAL_RUN', False)
DATA_DEST = os.environ.get('FETCHER_DATA_DEST', 's3')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
NUXEO_TOKEN = os.environ.get('NUXEO')

if not LOCAL_RUN and DATA_DEST == 'local':
    print(
        "A local data destination is only valid "
        "when the application is run locally"
    )
    exit()
