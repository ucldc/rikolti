import os
import requests
import json
import re

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


# repeating self here, how to get from avram?
RIGHTS_STATUS = {
    'CR': 'copyrighted',
    'PD': 'public domain',
    'UN': 'copyright unknown',
    # 'X':  'rights status unknown', # or should throw error?
}
RIGHTS_STATEMENT_DEFAULT = (
    "Please contact the contributing institution for more information "
    "regarding the copyright status of this object."
)
DCMI_TYPES = {
    'C': 'Collection',
    'D': 'Dataset',
    'E': 'Event',
    'I': 'Image',
    'F': 'Moving Image',
    'R': 'Interactive Resource',
    'V': 'Service',
    'S': 'Software',
    'A': 'Sound',   # A for audio
    'T': 'Text',
    'P': 'Physical Object',
    # 'X': 'type unknown' # default, not set
}


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

    def required_values_from_collection_registry(self, field, mode=None):
        collection = requests.get(
            f"https://registry.cdlib.org/api/v1/collection/"
            f"{self.collection_id}?format=json"
        ).json()

        if field == "rights":
            rights = [
                RIGHTS_STATUS.get(collection.get('rights_status')),
                collection.get('rights_statement')
            ]
            rights = [r for r in rights if r]
            if not rights:
                rights = [RIGHTS_STATEMENT_DEFAULT]
            field_value = rights

        if field == "type":
            field_value = [DCMI_TYPES.get(collection.get('dcmi_type'), None)]

        if field == "title":
            field_value = ["Title unknown"]

        if mode == "overwrite":
            self.mapped_metadata[field] = field_value
        elif mode == "append":
            if field in self.mapped_metadata:
                self.mapped_metadata[field] += (field_value)
            else:
                self.mapped_metadata[field] = field_value
        else:   # default is fill if empty
            if field not in self.mapped_metadata:
                self.mapped_metadata[field] = field_value

        # not sure what this is about
        # if not exists(data, "@context"):
            # self.mapped_metadata["@context"] = "http://dp.la/api/items/context"
        return self

    def shred(self, field, delim=";"):
        """
        Based on DPLA-Ingestion service that accepts a JSON document and
        "shreds" the value of the field named by the "prop" parameter

        Splits values by delimeter; handles some complex edge cases beyond what
        split() expects. For example:
            ["a,b,c", "d,e,f"] -> ["a","b","c","d","e","f"]
            'a,b(,c)' -> ['a', 'b(,c)']
        Duplicate values are removed.
        """
        if field not in self.mapped_metadata:
            return self

        value = self.mapped_metadata[field]
        if isinstance(value, list):
            try:
                value = delim.join(value)
                value = value.replace(f"{delim}{delim}", delim)
            except Exception as e:
                print(
                    f"Can't join list {value} on delim for "
                    f"{self.mapped_metadata['id']}, {e}"
                )
        if delim not in value:
            return self

        shredded = value.split(re.escape(delim))
        shredded = [s.strip() for s in shredded if s.strip()]
        result = []
        for s in shredded:
            if s not in result:
                result.append(s)
        self.mapped_metadata[field] = result

        return self