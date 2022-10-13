import os
import boto3
import json


class VernacularReader(object):
    def __init__(self, payload):
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def local_path(self, folder):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            folder, 
            str(self.collection_id),
        ])
        return local_path

    def get_local_api_response(self):
        local_path = self.local_path('vernacular_metadata')
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "r")
        api_response = page.read()
        return api_response

    def get_s3_api_response(self):
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"vernacular_metadata/{self.collection_id}/{self.page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        api_response = s3_obj_summary['Body'].read()
        return api_response


class UCLDCWriter(object):
    def __init__(self, payload):
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def local_path(self, folder):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            folder, 
            str(self.collection_id),
        ])
        return local_path

    def write_local_mapped_metadata(self, mapped_metadata):
        local_path = self.local_path('mapped_metadata')
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "w+")
        page.write(json.dumps(mapped_metadata))

    def write_s3_mapped_metadata(self, mapped_metadata):
        s3_client = boto3.client('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{self.collection_id}/{self.page_filename}"
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=json.dumps(mapped_metadata))


class Record(object):
    def __init__(self, col_id, record):
        self.collection_id = col_id
        self.source_metadata = record

    # Mapper Helpers
    def collate_subfield(self, field, subfield):
        return [f[subfield] for f in self.source_metadata.get(field, [])]

    # Enrichments
    def enrich(self, enrichment_function, **kwargs):
        func = getattr(self, enrichment_function)
        return func(**kwargs)

    def select_id(self, prop):
        if len(prop) > 1:
            raise Exception("select_id only accepts one property")
        if not isinstance(prop, list):
            raise Exception("select_id only accepts a list of length 1")
        prop = prop[0]

        # original dpla_ingestion/lib/akamod/select-id.py
        # does two things not implemented here:
        # 1. a tree traversal get instead of a simple .get(prop)
        # 2. handles if the id_handle is not just a simple string
        # not sure if we need to do either of these things, so
        # making a simple implementation for now

        id_handle = self.source_metadata.get(prop)
        lname = id_handle.strip().replace(" ", "__")

        self.legacy_couch_db_id = (f"{self.collection_id}--{lname}")
        return self
