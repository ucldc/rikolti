import json
from Fetcher import Fetcher, FetchError
import os
import requests
import urllib.parse

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
        self.nuxeo = params.get('nuxeo')
        if self.nuxeo.get('current_page_index') is None:
            self.nuxeo['current_page_index'] = 0
        if self.nuxeo.get('current_structural_type') is None:
            self.nuxeo['current_structural_type'] = 'parents'
        if self.nuxeo.get('folder_list') is None:
            self.nuxeo['folder_list'] = []
            self.build_folder_list()
        if self.nuxeo.get('current_nuxeo_path') is None:
            self.nuxeo['current_nuxeo_path'] = self.nuxeo.get('path')
        if self.nuxeo.get('current_nuxeo_uid') is None:
            self.nuxeo['current_nuxeo_uid'] = self.get_nuxeo_id(self.nuxeo['current_nuxeo_path'])
        if self.nuxeo.get('fetch_components') is None:
            self.nuxeo['fetch_components'] = True
        if self.nuxeo.get('parent_list') is None:
            self.nuxeo['parent_list'] = []
        if self.nuxeo.get('component_count') is None:
            self.nuxeo['component_count'] = 0
        if self.nuxeo.get('last_request_doc_count') is None:
            self.nuxeo['last_request_doc_count'] = 0

    def get_nuxeo_id(self, path):
        ''' get nuxeo uid of doc given path '''
        escaped_path = urllib.parse.quote(path, safe=' /')
        url = u'/'.join([API_BASE, API_PATH, "path", escaped_path.strip('/')])
        headers = NUXEO_REQUEST_HEADERS
        request = {'url': url, 'headers': headers}
        response = requests.get(**request)
        response.raise_for_status()
        json_response = response.json()

        return json_response['uid']

    def build_folder_list(self, current_page_index=0):
        ''' get list of subfolders for collection path '''
        http_resp = self.fetch_folders(current_page_index)
        json_response = http_resp.json()
        folders = [{'path': doc['path'], 'uid': doc['uid']} for doc in json_response['entries']]
        self.nuxeo['folder_list'].extend(folders)

        if json_response.get('isNextPageAvailable'):
            current_page_index = current_page_index + 1
            self.build_folder_list(current_page_index)

    def fetch_folders(self, current_page_index):
        query = RECURSIVE_FOLDER_NXQL.format(self.nuxeo.get('path'))
        url = u'/'.join([API_BASE, API_PATH, "path/@search"])
        headers = NUXEO_REQUEST_HEADERS
        params = {
            'pageSize': '100',
            'currentPageIndex': current_page_index,
            'query': query
        }
        request = {'url': url, 'headers': headers, 'params': params}
        response = requests.get(**request)
        response.raise_for_status()

        return response

    def build_fetch_request(self):
        page = self.nuxeo.get('current_page_index')
        if (page and page != -1) or page == 0:
            if self.nuxeo.get('current_structural_type') == 'parents':
                query = PARENT_NXQL.format(self.nuxeo.get('current_nuxeo_uid'))
            elif self.nuxeo.get('current_structural_type') == 'components':
                query = RECURSIVE_OBJECT_NXQL.format(self.nuxeo.get('current_nuxeo_path'))
            url = u'/'.join([API_BASE, API_PATH, "path/@search"])
            headers = NUXEO_REQUEST_HEADERS
            params = {
                'pageSize': '100',
                'currentPageIndex': self.nuxeo.get('current_page_index'),
                'query': query
            }
            request = {'url': url, 'headers': headers, 'params': params}
            print(
                f"Fetching page"
                f" {request.get('params').get('currentPageIndex')} "
                f"at {request.get('url')} "
                f"with query {request.get('params').get('query')} "
                f"for path {self.nuxeo.get('current_nuxeo_path')}"
                )
        else:
            request = None
            print("No more pages to fetch")

        return request

    def get_records(self, http_resp):
        response = http_resp.json()
        documents = [self.build_id(doc) for doc in response['entries']]

        if self.nuxeo.get('current_structural_type') == 'parents':
            for doc in response['entries']:
                self.nuxeo['parent_list'].append(
                    {
                        'path': doc['path'],
                        'uid': doc['uid']
                    }
                )

        if self.nuxeo.get('current_structural_type') == 'components':
            self.nuxeo['component_count'] = self.nuxeo['component_count'] + len(response['entries'])

        self.nuxeo['last_request_doc_count'] = len(documents)

        return documents

    def increment(self, http_resp):
        resp = http_resp.json()

        if resp.get('isNextPageAvailable'):
            self.write_page = self.write_page + 1
            current_page = self.nuxeo.get('current_page_index', 0)
            self.nuxeo['current_page_index'] = current_page + 1
            return

        if self.nuxeo['current_structural_type'] == 'parents':
            if len(self.nuxeo['folder_list']) > 0:
                self.increment_for_nested_parents()
                return
            else:
                self.nuxeo['parent_count'] = len(self.nuxeo['parent_list'])
                if self.nuxeo.get('fetch_components'):
                    self.nuxeo['current_structural_type'] = 'components'
                else:
                    print(f"TOTAL OBJECTS FETCHED: {self.nuxeo['parent_count']}")
                    self.nuxeo['current_page_index'] = -1
                    return

        if self.nuxeo['current_structural_type'] == 'components':
            if len(self.nuxeo['parent_list']) > 0:
                self.increment_for_components()
                return
            else:
                print(f"TOTAL PARENT OBJECTS FETCHED: {self.nuxeo['parent_count']}")
                print(f"TOTAL COMPONENTS FETCHED: {self.nuxeo['component_count']}")
                self.nuxeo['current_page_index'] = -1
                return

    def increment_for_nested_parents(self):
        self.nuxeo['current_nuxeo_path'] = self.nuxeo['folder_list'][0]['path'] + '/'
        self.nuxeo['current_nuxeo_uid'] = self.nuxeo['folder_list'][0]['uid']
        self.nuxeo['folder_list'].pop(0)

        self.nuxeo['current_page_index'] = 0

        if self.nuxeo['last_request_doc_count'] > 0:
            self.write_page = self.write_page + 1

    def increment_for_components(self):
        self.nuxeo['current_nuxeo_path'] = self.nuxeo['parent_list'][0]['path'] + '/'
        self.nuxeo['current_nuxeo_uid'] = self.nuxeo['parent_list'][0]['uid']
        self.nuxeo['parent_list'].pop(0)

        self.nuxeo['current_page_index'] = 0

        self.write_page = 0

    def json(self):
        if self.nuxeo.get('current_page_index') == -1:
            return None

        return json.dumps({
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "nuxeo": self.nuxeo
        })

    def build_id(self, document):
        calisphere_id = f"{self.collection_id}--{document.get('uid')}"
        document['calisphere-id'] = calisphere_id
        return document

    def get_local_path(self):
        basepath = super(NuxeoFetcher, self).get_local_path()
        if self.nuxeo.get('current_structural_type') == 'parents':
            return basepath
        else:
            components_path = os.path.join(basepath, "components")
            if not os.path.exists(components_path):
                os.mkdir(components_path)

            uid_path = os.path.join(basepath, "components", f"{self.nuxeo.get('current_nuxeo_uid')}")
            if not os.path.exists(uid_path):
                os.mkdir(uid_path)
            return uid_path

    def get_s3_key(self):
        collection_key = self.s3_data['Key']
        if self.nuxeo.get(('current_structural_type')) == 'parents':
            return collection_key
        else:
            return f"{collection_key}components/{self.nuxeo.get('current_nuxeo_uid')}/"


