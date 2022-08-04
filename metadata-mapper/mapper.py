import os
import boto3
import json

DEBUG = os.environ.get('DEBUG', False)

# {"collection_id": 26098, "mapper_type": "NuxeoMapper", vernacular_page_index=0}
# {"collection_id": 26098, "mapper_type": "NuxeoMapper"}
class Mapper(object):
    def __init__(self, params): 
        self.vernacular_page_index = params.get('vernacular_page_index', 0)
        self.collection_id = params.get('collection_id')
        self.mapper_type = params.get('mapper_type')
        
        vernacular_pages = self.list_local_pages() if DEBUG else self.list_s3_keys()
        try:
            self.page_number = vernacular_pages[self.vernacular_page_index]
        except IndexError:
            print("we've hit the end!")
            exit()

        if not self.collection_id:
            print("ERROR ERROR ERROR")
            print('collection_id required for mapping')
            exit()

    def get_local_page(self):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        page_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            'vernacular_metadata', 
            str(self.collection_id),
            str(self.page_number)
        ])

        page = open(page_path, "r")
        print(f'mapping {page_path}')
        vernacular_metadata = page.read()
        return vernacular_metadata
    
    def get_s3_page(self):
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"vernacular_metadata/{self.collection_id}/{self.page_number}"
        s3_obj_summary = s3.Object(bucket, key).get()
        vernacular_metadata = s3_obj_summary['Body'].read()
        return vernacular_metadata

    def list_local_pages(self):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        collection_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            'vernacular_metadata', 
            str(self.collection_id)    
        ])
        file_list = [f for f in os.listdir(collection_path) 
                     if os.path.isfile(os.path.join(collection_path, f))]
        return file_list

    def list_s3_vernacular_pages(self):
        s3 = boto3.resource('s3')
        rikolti_bucket = s3.Bucket('rikolti')
        obj_keys = rikolti_bucket.objects.filter(Prefix=f'vernacular_metadata/{self.collection_id}')
        return obj_keys

    def write_local_page(self, content):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        page_path = os.sep.join([
            parent_dir,
            'rikolti_bucket',
            'mapped_metadata',
            str(self.collection_id)
        ])
        if not os.path.exists(page_path):
            os.makedirs(page_path)
        page_path = os.sep.join([page_path, str(self.page_number)])
        page = open(page_path, "w+")
        page.write(json.dumps(content))

    def write_s3_page(self, content):
        s3_client = boto3.client('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{self.collection_id}/{self.page_number}"
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=json.dumps(content))
    
    def map_page(self, vernacular_page):
        records = self.get_records(vernacular_page)
        mapped_records = [self.map_record(record) for record in records]
        return mapped_records

    def get_records(self, vernacular_page):
        pass

    def map_record(self, record):
        pass

    def increment(self):
        return {
            'collection_id': self.collection_id,
            'mapper_type': self.mapper_type,
            'vernacular_page_index': self.vernacular_page_index + 1
        }
