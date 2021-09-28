import json
from Fetcher import Fetcher, FetchError

import os
TOKEN = os.environ['NUXEO']


class NuxeoFetcher(Fetcher):
    def __init__(self, params):
        super(NuxeoFetcher, self).__init__(params)
        self.nuxeo = params.get('nuxeo')
        if not self.nuxeo.get('current_page_index'):
            self.nuxeo['current_page_index'] = 0
        if not self.nuxeo.get('current_structural_type'):
            self.nuxeo['current_structural_type'] = 'parents'
        if not self.nuxeo.get('current_nuxeo_path'):
            self.nuxeo['current_nuxeo_path'] = self.nuxeo.get('path')
        if not self.nuxeo.get('current_nuxeo_uid'):
            self.nuxeo['current_nuxeo_uid'] = ''
        if not self.nuxeo.get('parent_list'):
            self.nuxeo['parent_list'] = []

    def build_fetch_request(self):
        page = self.nuxeo.get('current_page_index')
        if (page and page != -1) or page == 0:
            url = (
                f"https://nuxeo.cdlib.org/Nuxeo/site/api/v1/"
                f"repo/default/path{self.nuxeo.get('current_nuxeo_path')}@children"
            )
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-NXDocumentProperties": "*",
                "X-NXRepository": "default",
                "X-Authentication-Token": TOKEN
            }
            params = {
                'pageSize': '100',
                'currentPageIndex': self.nuxeo.get('current_page_index')
            }
            request = {'url': url, 'headers': headers, 'params': params}
            print(
                f"Fetching page"
                f" {request.get('params').get('currentPageIndex')} "
                f"at {request.get('url')}")
        else:
            request = None
            print("No more pages to fetch")

        return request

    def get_records(self, http_resp):
        response = http_resp.json()
        print(response)
        documents = [self.build_id(doc) for doc in response['entries']]
        for doc in response['entries']:
            self.nuxeo['parent_list'].append(
                {
                    'path': doc['path'],
                    'uid': doc['uid']
                }
                
            )
        return documents

    def increment(self, http_resp):
        #super(NuxeoFetcher, self).increment(http_resp)
        resp = http_resp.json()
        # if next page is available, whether parent or component, fetch it
        if resp.get('isNextPageAvailable'):
            self.write_page = self.write_page + 1
            current_page = self.nuxeo.get('current_page_index', 0)
            self.nuxeo['current_page_index'] = current_page + 1
        else:
            # end of parents and we're not fetching components
            if not self.nuxeo.get('fetch_components'):
                self.nuxeo['current_page_index'] = -1
            # we are fetching components
            else:
                # fetch components for next item
                if self.nuxeo.get('current_structural_type') == 'parents':
                    self.nuxeo['current_structural_type'] = 'components'
                if len(self.nuxeo['parent_list']) > 0:
                    self.nuxeo['current_nuxeo_path'] = self.nuxeo['parent_list'][0]['path'] + '/'
                    self.nuxeo['current_nuxeo_uid'] = self.nuxeo['parent_list'][0]['uid']
                    self.nuxeo['parent_list'].pop(0)
                    self.nuxeo['current_page_index'] = 0
                else:
                    self.nuxeo['current_page_index'] = -1

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

    '''
    def fetchtolocal(self, records):
        if self.nuxeo.get('current_structural_type') == 'parents':
            super(NuxeoFetcher, self).fetchtolocal(records)
        else:
            path = os.path.join(
                os.getcwd(), 
                f"{self.collection_id}", 
                "components", 
                f"{self.nuxeo.get('current_nuxeo_uid')}"
            )
            if not os.path.exists(path):
                os.mkdir(path)
    '''

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







