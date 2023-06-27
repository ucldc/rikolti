import os
from indexer import bulk_add
import settings
import boto3
import json


# TODO: make use of utilities.py
def get_file_list(collection_id):

    file_list = []

    if settings.DATA_SRC == 'local':
        path = settings.local_path('mapped_with_content', collection_id)
        try:
            file_list = [f for f in os.listdir(path)
                            if os.path.isfile(os.path.join(path, f))]
        except FileNotFoundError as e:
            print(f"{e} - have you run content fetcher for {collection_id}?")
    else:
        # TODO: instantiate one boto3 client and pass it around
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            region_name=settings.AWS_REGION
        )
        response = s3_client.list_objects_v2(
            Bucket=settings.S3_BUCKET,
            Prefix=f'mapped_with_content/{collection_id}/'
        )
        file_list = [obj['Key'].split('/')[-1] for obj in response['Contents']]

    return file_list


def get_metadata(collection_id, filename):
    # TODO: instantiate one boto3 client and pass it around
    if settings.DATA_SRC == 'local':
        local_path = settings.local_path(
            'mapped_with_content', collection_id)
        path = os.path.join(local_path, str(filename))
        file = open(path, "r")
        record = json.loads(file.read())
        # s3_client.download_file(
        #     settings.S3_BUCKET,
        #     f"mapped_with_content/{collection_id}/{filename}",
        #     (
        #         "/usr/local/dev/rikolti/rikolti_bucket/mapped_with_content/"
        #         f"{collection_id}/{filename}"
        #     )
        # )
    else:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            region_name=settings.AWS_REGION
        )
        file = s3_client.get_object(
            Bucket=settings.S3_BUCKET,
            Key=f"mapped_with_content/{collection_id}/{filename}"
        )
        record = json.loads(file['Body'].read())
    
    return record


def index_collection(collection_id):

    print(f"{settings.DATA_SRC=}")

    file_list = get_file_list(collection_id)

    print(f"Indexing records for collection {collection_id}")

    records = [get_metadata(collection_id, file) for file in file_list]

    index_name = "rikolti-test"

    bulk_add(records, index_name)
