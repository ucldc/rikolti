import json
from Fetcher import Fetcher, FetchError
import os
import requests
from urllib.parse import quote as urllib_quote
import boto3
import settings
import subprocess


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
    ''' get nuxeo {path, uid} for doc at given path '''
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
            if settings.LOCAL_STORE:
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
            # prefix starts as ['r'] (read as "root")
            # as we traverse the tree, we add ["fp-0", "f-0"]
            # read as [root, folder page 0, folder 0]
            # 
            # api_page is the current page we are on - regardless
            # of query type, but we only actually produce an output file
            # when the query type is a document or child document
            # in which case, api_page corresponds to document page
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

    def check_page(self, http_resp):
        """Checks that the http_resp contains metadata records
        
        Also recurses down into documents & folders, calling
        a new fetching process to retrieve children of
        each document and documents inside each folder.
        Prefix is set to fp (folder page, since folders can
        paginate) and f (folder count) - and can be read as
        "folder page 0 - folder 1"

        Returns:
            A boolean indicating if the page contains records
        """
        response = http_resp.json()
        query_type = self.nuxeo.get('query_type')

        documents = False
        if query_type in ['documents', 'children'] and response.get('entries'):
            print(
                f"Fetched page {self.nuxeo.get('api_page')} of "
                f"{query_type} at {self.nuxeo.get('current_path')['path']}"
                f"with {len(response.get('entries'))} records"
            )
            documents = True

        if ((query_type == 'documents' and self.nuxeo['fetch_children'])
                or query_type == 'folders'):
            next_qt = 'children' if query_type == 'documents' else 'documents'
            for i, entry in enumerate(response.get('entries')):
                self.recurse(
                    path={
                        'path': entry.get('path'),
                        'uid': entry.get('uid')
                    }, 
                    query_type=next_qt, 
                    prefix=(
                       self.nuxeo['prefix'] +
                       [f"fp-{self.nuxeo['api_page']}", f'f-{i}']
                    )
                )

        return documents

    def increment(self, http_resp):
        """Increment the request given an http_resp 

        Checks isNextPageAvailable in the http_resp and increases
        api_page by 1
        
        Also kicks off a new lambda function to look for folders
        in the current folder, if we've finished fetching all the
        documents in the current folder

        Sets self.nuxeo to None if no next page available
        """
        resp = http_resp.json()
        query_type = self.nuxeo.get('query_type')
        has_next_page = resp.get('isNextPageAvailable')

        if query_type == 'documents' and not has_next_page:
            self.recurse(query_type='folders')

        if has_next_page:
            self.nuxeo['api_page'] += 1
            self.write_page = 0
        else:
            self.nuxeo = None

    def json(self):
        if not self.nuxeo:
            return None

        return json.dumps({
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "nuxeo": self.nuxeo
        })

    def recurse(self, path=None, query_type=None, prefix=None):
        """Starts a new lambda function"""
        lambda_query = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": 0,
            "nuxeo": {
                'path': self.nuxeo['path'],
                'fetch_children': self.nuxeo['fetch_children'],
                'current_path': path if path else self.nuxeo['current_path'],
                'query_type': (query_type if query_type else 
                               self.nuxeo['query_type']),
                'api_page': 0,
                'prefix': prefix if prefix else self.nuxeo['prefix']
            }
        }
        if settings.LOCAL_RUN:
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
