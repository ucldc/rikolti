import json
import requests
from Fetcher import Fetcher, FetchError

class JsonOAC(Fetcher):
    # params['oac'] = {
    #     "url": ("http://dsc.cdlib.org/search?facet=type-tab"
    #             "&style=cui&raw=1&relation=ark:/13030/kt28702559"),
    #     "counts": {
    #         "total": 145,
    #         "image": 128,
    #         "text": 17,
    #         "harvested": 0,
    #         "harvested_image": 100,
    #         "harvested_text": 0
    #     },
    #     "current_group": "image"
    # }
    def __init__(self, params):
        super(JsonOAC, self).__init__(params)
        self.oac = params.get('oac')

        # url = self.oac.get('url')
        # counts = self.oac.get('counts')
        # current_group = self.oac.get('current_group')

        if not self.oac.get('counts'):
            self.oac['counts'] = 1

    def build_fetch_request(self):
        url = self.oac.get('url')
        harvested = self.oac.get('counts')

        request = {"url": (
            f"{url}"#&docsPerPage=100"
            f"&startDoc={harvested}"
            # f"&group={current_group}"
        )}
        print(
            f"{self.collection_id}: Fetching page "
            f"at {request.get('url')}")

        return request

    def get_records(self, http_resp):
        response = json.loads(http_resp.content)
        documents = []
        for doc in response.get('objset'):
            doc['calisphere-id'] = self.build_id(doc)
            documents.append(doc)
        return documents

    def build_id(self, document):
        '''Return the object's ark from the xml etree docHit'''
        ids = document.get('identifier')
        ark = None
        for i in ids:
            split = i.split('ark:')
            if len(split) > 1:
                ark = ''.join(('ark:', split[1]))
                
        return f"{self.collection_id}--{ark}"

    def increment(self, http_resp):
        super(JsonOAC, self).increment(http_resp)
        response = json.loads(http_resp.content)
        self.oac['counts'] = int(response.get('objset_end'))
        if self.oac['counts'] < response.get('objset_total'):
            self.oac['current_group'] = 'objset'
        else:
            self.oac['current_group'] = None

        return

    def json(self):
        if not self.oac.get('current_group'):
            return None

        return json.dumps({
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "oac": self.oac
        })
