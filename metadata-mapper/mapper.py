import os
import boto3
import json

DEBUG = os.environ.get('DEBUG', False)

# {"collection_id": 26098, "mapper_type": "NuxeoMapper", vernacular_page_index=0}
# {"collection_id": 26098, "mapper_type": "NuxeoMapper"}
class Mapper(object):
    def __init__(self, params): 
        self.page_filename = params.get('page_filename', 0)
        self.collection_id = params.get('collection_id')
        self.mapper_type = params.get('mapper_type')
        
        if not self.collection_id:
            print("ERROR ERROR ERROR")
            print('collection_id required for mapping')
            exit()

    def local_path(self, folder):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            folder, 
            str(self.collection_id),
        ])
        return local_path

    def read_vernacular(self):
        if DEBUG:
            local_path = self.local_path('vernacular_metadata')
            page_path = os.sep.join([local_path, str(self.page_filename)])
            page = open(page_path, "r")
            vernacular_metadata = page.read()
        else:
            s3 = boto3.resource('s3')
            bucket = 'rikolti'
            key = f"vernacular_metadata/{self.collection_id}/{self.page_filename}"
            s3_obj_summary = s3.Object(bucket, key).get()
            vernacular_metadata = s3_obj_summary['Body'].read()

        return vernacular_metadata

    def write_mapped(self, content):
        if DEBUG:
            local_path = self.local_path('mapped_metadata')
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            page_path = os.sep.join([local_path, str(self.page_filename)])
            page = open(page_path, "w+")
            page.write(json.dumps(content))
        else:
            s3_client = boto3.client('s3')
            bucket = 'rikolti'
            key = f"mapped_metadata/{self.collection_id}/{self.page_filename}"
            s3_client.put_object(
                ACL='bucket-owner-full-control',
                Bucket=bucket,
                Key=key,
                Body=json.dumps(content))

    def map_page(self):
        print(f"Mapping page {self.page_filename} of collection {self.collection_id}")
        vernacular_page = self.read_vernacular()
        records = self.get_records(vernacular_page)
        mapped_records = [self.map_record(record) for record in records]
        self.write_mapped(mapped_records)
        print(f"Finished mapping page {self.page_filename} of collection {self.collection_id}")

    def get_records(self, vernacular_page):
        pass

    def map_record(self, record):
        pass

    def increment(self):
        return {
            'collection_id': self.collection_id,
            'mapper_type': self.mapper_type,
            'page_filename': self.page_filename + 1
        }
    
