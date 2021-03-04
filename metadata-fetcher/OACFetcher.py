import asyncio
import xmltodict
import json
from Fetcher import Fetcher

GROUPS_TO_FETCH = ['image', 'text']

class OACFetcher(Fetcher):
    def __init__(self, params):
        super(OACFetcher, self).__init__(params)
        self.oac = params.get('oac')
        self.start_doc = 0
        self.groups = None
        self.oac['done'] = False

        # get total docs, groups
        try:
            self.base = f"{self.oac.get('url')}&docsPerPage=100"
        except KeyError:
            print('no oac feed url')


    async def build_fetch_request(self):

        # initial request
        if not self.groups:
            self.groups = self.get_groups()
            # handle None
            self.oac['groupIndex'] = 0
            self.current_group = self.groups[0]
            # handle no images and/or no text
            self.oac['startDoc'] = 1
            self.oac['currentGroupName'] = self.current_group.get('groupname')
            url = f"{self.base}&startDoc={self.oac['startDoc']}&group={self.oac['currentGroupName']}"
        # subsequent requests
        elif self.oac['done'] is False:
            url = f"{self.base}&startDoc={self.oac['startDoc']}&group={self.oac['currentGroupName']}"
        else:
            print("No more pages to fetch")
            return None

        # url = f"{self.base}&startDoc={startDoc}&group={currentGroup}"

        print(f"Fetching page {self.write_page} at {url}")
        return {"url": url}

    def get_groups(self):
        
        # skip groups not in `groups_to_fetch`
        groups = [{'groupname': 'image', 'count': 25, 'current_start_doc': 1, 'current_end_doc': 0}]
        # return None if there aren't any
        return groups


    async def get_records(self, httpResp):
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        records = data.get('crossQueryResult', {}) .get('facet', {}).get('group'), {}.get('docHit')

        return records


    async def increment(self, httpResp):
        
        await super(OACFetcher, self).increment(httpResp) # not sure why OAIFetcher.py doesn't need positional arg?
        
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        totalDocs = int(data.get('crossQueryResult').get('facet').get('group')[0]['@totalDocs'])
        endDoc = int(data.get('crossQueryResult').get('facet').get('group')[0]['@endDoc'])

        if endDoc >= totalDocs:
            if self.oac['groupIndex'] + 1 >= len(self.groups):
                self.oac['done'] = True
                return None
            else:
                self.oac['groupIndex'] = self.oac['groupIndex'] + 1
                self.current_group = self.groups[self.oac['groupIndex']]
                self.oac['startDoc'] = 1
                self.oac['currentGroupName'] = self.current_group.get('groupname')
        else:
            self.oac['startDoc'] = endDoc + 1

