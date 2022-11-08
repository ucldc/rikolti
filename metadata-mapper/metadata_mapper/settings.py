import os

LOCAL_RUN = os.environ.get('MAPPER_LOCAL_RUN', False)
DATA_SRC = os.environ.get('MAPPER_DATA_SOURCE', False)
DATA_DEST = os.environ.get('MAPPER_DATA_DEST', False)

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)

if not LOCAL_RUN and (DATA_SRC == 'local' or DATA_DEST == 'local'):
    print(
        "A local data source or local data destination "
        "is only valid when the application is run locally"
    )
    exit()

