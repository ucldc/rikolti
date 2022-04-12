import json
from Fetcher import Fetcher, FetchError
import os
import requests
from urllib.parse import quote as urllib_quote
import boto3
import subprocess

DEBUG = os.environ.get('DEBUG', False)

TOKEN = os.environ.get('NUXEO')
API_BASE = 'https://nuxeo.cdlib.org/Nuxeo/site'
API_PATH = 'api/v1'

RECURSIVE_FOLDER_NXQL = "SELECT * FROM Organization " \
                        "WHERE ecm:path STARTSWITH '{}' " \
                        "AND ecm:isTrashed = 0"

# for some reason, using `ORDER BY ecm:name` in the query avoids the
# bug where the API was returning duplicate records from Nuxeo
PARENT_NXQL = "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
              "WHERE ecm:parentId = '{}' AND " \
              "ecm:isTrashed = 0 ORDER BY ecm:name"

RECURSIVE_OBJECT_NXQL = "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
                       "WHERE ecm:path STARTSWITH '{}' " \
                       "AND ecm:isTrashed = 0 " \
                       "ORDER BY ecm:pos"

NUXEO_REQUEST_HEADERS = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-NXDocumentProperties": "*",
                "X-NXRepository": "default",
                "X-Authentication-Token": TOKEN
                }


class NuxeoFetcher(Fetcher):
    def __init__(self, params):
        super(NuxeoFetcher, self).__init__(params)

        nuxeo_defaults = {
            'query_type': 'documents',
            'current_path': None,
            'api_page': 0,
            'prefix': []
        }
        nuxeo_defaults.update(params.get('nuxeo'))
        self.nuxeo = nuxeo_defaults

        if not self.nuxeo.get('current_path'):
            # get nuxeo uid for doc at given path
            escaped_path = urllib_quote(self.nuxeo.get('path'), safe=' /')
            request = {
                'url': f"{API_BASE}/{API_PATH}/path/{escaped_path.strip('/')}", 
                'headers': NUXEO_REQUEST_HEADERS
            }
            response = requests.get(**request)
            response.raise_for_status()
            uid = response.json().get('uid')
            self.nuxeo['current_path'] = {
                'path': self.nuxeo.get('path'), 
                'uid': uid
            }

        self.page_holder = self.write_page
        write_page = [str(p) for p in self.nuxeo.get('prefix')] + [str(self.write_page)]
        self.write_page = '-'.join(write_page)

    def build_fetch_request(self):
        query_type = self.nuxeo.get('query_type')
        current_path = self.nuxeo.get('current_path')

        if query_type == 'documents':
            query = PARENT_NXQL.format(current_path['uid'])
            page = self.nuxeo.get('api_page')
        # if query_type == 'children':
        #     query = RECURSIVE_OBJECT_NXQL.format(current_path['path'])
        #     page = self.nuxeo.get('child_page')
        if query_type == 'folders':
            query = RECURSIVE_FOLDER_NXQL.format(current_path['path'])
            page = self.nuxeo.get('api_page')

        request = {
            'url': f"{API_BASE}/{API_PATH}/path/@search", 
            'headers': NUXEO_REQUEST_HEADERS, 
            'params': {
                'pageSize': '100',
                'currentPageIndex': page,
                'query': query
            }
        }

        print(
            f"--------------------------------\n"
            f"{self}\n"
            f"Fetching page {page} of {query_type}\n"
            f"    {current_path}\n"
        )

        return request

    def get_records(self, http_resp):
        query_type = self.nuxeo.get('query_type')
        resp = http_resp.json()

        documents = []
        if query_type in ['documents', 'children']:
            documents = [self.build_id(doc) for doc in resp.get('entries')]

        folders = []
        if query_type == 'folders':
            folders = resp.get('entries')

        print(
            f"--------------------------------\n"
            f"{self}\n"
            f"{len(resp.get('entries'))} entries: "
            f"{len(documents)} documents, {len(folders)} folders\n"
        )
        return documents

    def increment(self, http_resp):
        resp = http_resp.json()
        query_type = self.nuxeo.get('query_type')
        next_page_bool = resp.get('isNextPageAvailable')

        if next_page_bool:
            self.nuxeo['api_page'] += 1
            self.write_page = self.page_holder + 1
        
        if query_type == 'documents' and not next_page_bool:
            folder_query = json.loads(self.json())
            folder_query['nuxeo']['query_type'] = 'folders'
            folder_query['nuxeo']['api_page'] = 0
            folder_query['write_page'] = 0

            if DEBUG:
                print(
                    f"--------------------------------\n"
                    f"{self}\n"
                    "no more documents - getting folders:\n"
                    f"{json.dumps(folder_query, indent=4)}\n"
                )
                subprocess.run([
                    'python',
                    'lambda_function.py',
                    json.dumps(folder_query).encode('utf-8')
                ])
            else:
                lambda_client = boto3.client('lambda', region_name="us-west-2",)
                lambda_client.invoke(
                    FunctionName="fetch-metadata",
                    InvocationType="Event",  # invoke asynchronously
                    Payload=json.dumps(folder_query).encode('utf-8')
                )
        
        if query_type == 'folders':
            folders = [{
                'path': entry.get('path'), 
                'uid': entry.get('uid')
            } for entry in resp.get('entries')]
            for i, folder in enumerate(folders):
                sub_folder_query = json.loads(self.json())
                sub_folder_query['nuxeo']['query_type'] = 'documents'
                sub_folder_query['nuxeo']['current_path'] = folder
                sub_folder_query['nuxeo']['api_page'] = 0
                sub_folder_query['write_page'] = 0
                sub_folder_query['nuxeo']['prefix'] = [i] + sub_folder_query['nuxeo']['prefix']
                if DEBUG:
                    print(
                        f"--------------------------------\n"
                        f"{self}\n"
                        f"fetching documents for folder {folder}:"
                        f"{json.dumps(sub_folder_query, indent=4)}\n"
                    )
                    subprocess.run([
                        'python',
                        'lambda_function.py',
                        json.dumps(sub_folder_query).encode('utf-8')
                    ])
                else:
                    lambda_client = boto3.client('lambda', region_name="us-west-2",)
                    lambda_client.invoke(
                        FunctionName="fetch-metadata",
                        InvocationType="Event",  # invoke asynchronously
                        Payload=json.dumps(sub_folder_query).encode('utf-8')
                    )

        if not next_page_bool:
            self.nuxeo = None

    def json(self):
        if not self.nuxeo:
            return None

        next_page = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "nuxeo": self.nuxeo
        }

        return json.dumps(next_page)

    def build_id(self, document):
        calisphere_id = f"{self.collection_id}--{document.get('uid')}"
        document['calisphere-id'] = calisphere_id
        return document

    def __str__(self):
        return (
            f"{self.nuxeo['current_path'].get('path')} - "
            f"{self.nuxeo.get('query_type')} - "
            f"{self.nuxeo.get('api_page')} - "
            f"{self.nuxeo.get('prefix')} - "
            f"{self.write_page}"
        )
