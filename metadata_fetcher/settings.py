import os

LOCAL_RUN = os.environ.get('FETCHER_LOCAL_RUN', False)
LOCAL_STORE = os.environ.get('FETCHER_LOCAL_STORE', False)
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')    # doesn't currently do anything
S3_BUCKET = os.environ.get('S3_BUCKET', False)
TOKEN = os.environ.get('NUXEO')

if not LOCAL_RUN and LOCAL_STORE:
    print("FETCH_LOCALLY is only valid when RUN_LOCALLY is set")
    exit()

# run locally, or on AWS Lambda
# fetch locally, or to AWS s3
# verbose output for debugging/log levels
# if running on AWS Lambda but fetching locally, ERROR