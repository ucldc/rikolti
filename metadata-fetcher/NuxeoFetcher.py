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


def get_path_uid(path):
    # get nuxeo uid for doc at given path
    escaped_path = urllib_quote(path, safe=' /')
    request = {
        'url': f"{API_BASE}/{API_PATH}/path/{escaped_path.strip('/')}",
        'headers': NUXEO_REQUEST_HEADERS
    }
    response = requests.get(**request)
    response.raise_for_status()
    uid = response.json().get('uid')
    current_path = {
        'path': path,
        'uid': uid
    }
    return current_path


def recurse(lambda_query):
    if DEBUG:
        subprocess.run([
            'python',
            'lambda_function.py',
            json.dumps(lambda_query).encode('utf-8')
        ])
    else:
        lambda_client = boto3.client('lambda', region_name="us-west-2",)
        lambda_client.invoke(
            FunctionName="fetch-metadata",
            InvocationType="Event",  # invoke asynchronously
            Payload=json.dumps(lambda_query).encode('utf-8')
        )


class NuxeoFetcher(Fetcher):
    def __init__(self, params):
        super(NuxeoFetcher, self).__init__(params)

        nuxeo_defaults = {
            'path': None,
            'fetch_children': True,
            'current_path': None,
            'query_type': 'documents',
            'api_page': 0,
            'prefix': ['r'],
        }
        nuxeo_defaults.update(params.get('nuxeo'))

        self.nuxeo = nuxeo_defaults
        if not self.nuxeo.get('current_path'):
            self.nuxeo['current_path'] = get_path_uid(self.nuxeo.get('path'))

        if self.nuxeo['query_type'] == 'children':
            if DEBUG:
                path = self.get_local_path()
                children_path = os.path.join(path, "children")
                if not os.path.exists(children_path):
                    os.mkdir(children_path)
            self.write_page = (
                "children/"
                f"{self.nuxeo['current_path']['uid']}-"
                f"{self.nuxeo['api_page']}"
            )
        else:
            write_page = self.nuxeo['prefix'] + [f"{self.nuxeo['api_page']}"]
            self.write_page = '-'.join(write_page)

    def build_fetch_request(self):
        query_type = self.nuxeo.get('query_type')
        current_path = self.nuxeo.get('current_path')
        page = self.nuxeo.get('api_page')

        if query_type == 'documents':
            query = PARENT_NXQL.format(current_path['uid'])
        if query_type == 'children':
            query = RECURSIVE_OBJECT_NXQL.format(current_path['path'])
        if query_type == 'folders':
            query = RECURSIVE_FOLDER_NXQL.format(current_path['path'])

        request = {
            'url': f"{API_BASE}/{API_PATH}/path/@search", 
            'headers': NUXEO_REQUEST_HEADERS, 
            'params': {
                'pageSize': '100',
                'currentPageIndex': page,
                'query': query
            }
        }
        print(f"Fetching page {page} of {query_type} at {current_path['path']}")

        return request

    def get_records(self, http_resp):
        query_type = self.nuxeo.get('query_type')
        resp = http_resp.json()

        documents = []
        if query_type in ['documents', 'children']:
            documents = [self.build_id(doc) for doc in resp.get('entries')]

        if ((query_type == 'documents' and self.nuxeo['fetch_children'])
                or query_type == 'folders'):

            down = None
            if query_type == 'documents':
                down = 'children'
            if query_type == 'folders':
                down = 'documents'

            for i, entry in enumerate(resp.get('entries')):
                recurse({
                    "harvest_type": self.harvest_type,
                    "collection_id": self.collection_id,
                    "write_page": 0,
                    "nuxeo": {
                        'path': self.nuxeo['path'],
                        'fetch_children': self.nuxeo['fetch_children'],
                        'current_path': {
                            'path': entry.get('path'),
                            'uid': entry.get('uid')
                        },
                        'query_type': down,
                        'api_page': 0,
                        'prefix': (
                            self.nuxeo['prefix'] +
                            [f"fp{self.nuxeo['api_page']}", f'f{i}']
                        )
                    }
                })

        return documents

    def increment(self, http_resp):
        resp = http_resp.json()
        query_type = self.nuxeo.get('query_type')
        next_page_bool = resp.get('isNextPageAvailable')

        if query_type == 'documents' and not next_page_bool:
            recurse({
                "harvest_type": self.harvest_type,
                "collection_id": self.collection_id,
                "write_page": 0,
                "nuxeo": {
                    'path': self.nuxeo['path'],
                    'fetch_children': self.nuxeo['fetch_children'],
                    'current_path': self.nuxeo['current_path'],
                    'query_type': 'folders',
                    'api_page': 0,
                    'prefix': self.nuxeo['prefix'],
                }
            })

        if next_page_bool:
            self.nuxeo['api_page'] += 1
            self.write_page = 0
        else:
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
