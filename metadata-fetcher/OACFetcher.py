import asyncio
import xmltodict
import json
import aiohttp
from Fetcher import Fetcher

GROUPS_TO_FETCH = ['image', 'text']

class OACFetcher(Fetcher):
    def __init__(self, params):
        super(OACFetcher, self).__init__(params)
        self.oac = params.get('oac')
        self.oac['groups'] = None
        self.oac['done'] = False

        # get total docs, groups
        try:
            self.base = f"{self.oac.get('url')}&docsPerPage=100"
        except KeyError:
            print('no oac feed url')


    async def build_fetch_request(self):
        # initial request
        if not self.oac['groups']:
            self.oac['groups'] = await self.get_groups()
            if len(self.oac['groups']) == 0:
                print("No valid group data found")
                return None
            self.oac['current_group_index'] = 0
            self.oac['current_group'] = self.oac['groups'][self.oac['current_group_index']]
            self.oac['start_doc'] = 1
            self.oac['current_group_name'] = self.oac['current_group'].get('groupname')
            url = f"{self.base}&startDoc={self.oac['start_doc']}&group={self.oac['current_group_name']}"
        elif self.oac['done'] is False:
            url = f"{self.base}&startDoc={self.oac['start_doc']}&group={self.oac['current_group_name']}"
        else:
            print("No more pages to fetch")
            return None

        print(f"Fetching page {self.write_page} at {url}")
        return {"url": url}

    async def get_groups(self):
        groups = []

        # FIXME bad to have multiple aiohttp.ClientSession's going at once??
        async with aiohttp.ClientSession() as http_client:

            async with http_client.get(self.base) as response:
                resp = await response.text()
                data = xmltodict.parse(resp)

                # get all group facets with 1 or more docs
                group_facets = data.get('crossQueryResult', {}) .get('facet', {}).get('group')
                for g in group_facets:
                    if g.get('@totalDocs') and int(g.get('@totalDocs')) > 0:
                        group = {}
                        group['groupname'] = g.get('@value')
                        group['count'] = int(g.get('@totalDocs'))
                        group['current_start_doc'] = 1
                        group['current_end_doc'] = 0
                        groups.append(group)
        
        return groups


    async def get_records(self, httpResp):
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        records = []
        groups = data.get('crossQueryResult').get('facet').get('group')
        for group in groups:
            if group.get('@value') == self.oac['current_group_name']:
                doc_hit = group.get('docHit', [])
                for doc in doc_hit:
                    meta = doc.get("meta", {})
                    # the first item is an empty list, so skip it
                    if isinstance(meta, dict):
                        records.append(meta)

        return records


    async def increment(self, httpResp):
        await super(OACFetcher, self).increment(httpResp)
        
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        # FIXME need to get current group, not just [0], I think
        total_docs = int(data.get('crossQueryResult').get('facet').get('group')[0]['@totalDocs'])
        end_doc = int(data.get('crossQueryResult').get('facet').get('group')[0]['@endDoc'])

        if end_doc >= total_docs:
            if self.oac['current_group_index'] + 1 >= len(self.oac['groups']):
                self.oac['done'] = True
                return None
            else:
                self.oac['current_group_index'] = self.oac['current_group_index'] + 1
                self.oac['current_group'] = self.groups[self.oac['current_group_index']]
                self.oac['start_doc'] = 1
                self.oac['current_group_name'] = self.oac['current_group'].get('groupname')
        else:
            self.oac['start_doc'] = end_doc + 1

