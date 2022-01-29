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

    def build_fetch_request(self):
        page = self.nuxeo.get('current_page_index')
        if (page and page != -1) or page == 0:
            url = (
                f"https://nuxeo.cdlib.org/Nuxeo/site/api/v1/"
                f"repo/default/path{self.nuxeo.get('path')}@children"
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
        return documents

    def increment(self, http_resp):
        super(NuxeoFetcher, self).increment(http_resp)
        resp = http_resp.json()
        if resp.get('isNextPageAvailable'):
            current_page = self.nuxeo.get('current_page_index', 0)
            self.nuxeo['current_page_index'] = current_page + 1
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
