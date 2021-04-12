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
            self.oac['start_doc'] = 1
            url = f"{self.base}&startDoc={self.oac['start_doc']}&group={self.oac['groups'][self.oac['current_group_index']].get('groupname')}"
        elif self.oac['done'] is False:
            url = f"{self.base}&startDoc={self.oac['start_doc']}&group={self.oac['groups'][self.oac['current_group_index']].get('groupname')}"
        else:
            print("No more pages to fetch")
            return None

        print(f"Fetching page {self.write_page} at {url}")
        return {"url": url}

    async def get_groups(self):
        '''
        The oac urls provided in the registry are for a faceted query
        Results are returned in groups: image, text, website
        We are only interested in image and text
        '''
        groups = []

        # FIXME bad to have multiple aiohttp.ClientSession's going at once??
        async with aiohttp.ClientSession() as http_client:
            async with http_client.get(self.base) as response:
                resp = await response.text()
                data = xmltodict.parse(resp)

                # get all valid group facets with 1 or more docs
                group_facets = data.get('crossQueryResult', {}) .get('facet', {}).get('group')
                for g in group_facets:
                    if g.get('@value') in GROUPS_TO_FETCH and int(g.get('@totalDocs')) > 0:
                        group = {}
                        group['groupname'] = g.get('@value')
                        group['count'] = int(g.get('@totalDocs'))
                        groups.append(group)
        
        return groups


    async def get_records(self, httpResp):
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        raw_records = []
        group_facets = data.get('crossQueryResult').get('facet').get('group')
        for g in group_facets:
            if g.get('@value') == self.oac['groups'][self.oac['current_group_index']].get('groupname'):
                doc_hit = g.get('docHit', [])
                for doc in doc_hit:
                    meta = doc.get("meta", {})
                    # the first item is an empty list, so skip it
                    if isinstance(meta, dict):
                        raw_records.append(meta)

        records = [await self.build_id(doc) for doc in raw_records]

        return records


    async def increment(self, httpResp):
        await super(OACFetcher, self).increment(httpResp)
        
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        group_facets = data.get('crossQueryResult').get('facet').get('group')
        for g in group_facets:
            if g.get('@value') == self.oac['groups'][self.oac['current_group_index']].get('groupname'):
                total_docs = int(g['@totalDocs'])
                end_doc = int(g['@endDoc'])

        if end_doc >= total_docs:
            if self.oac['current_group_index'] + 1 >= len(self.oac['groups']):
                self.oac['done'] = True
                return None
            else:
                self.oac['current_group_index'] = self.oac['current_group_index'] + 1
                self.oac['start_doc'] = 1
        else:
            self.oac['start_doc'] = end_doc + 1


    async def build_id(self, document):
        '''
        based on https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/master/lib/akamod/select_oac_id.py
        '''
        ark = None
        for identifier in document.get('identifier'):
            if isinstance(identifier, str):
                ark = identifier
                break
            else:
                for value in (identifier if isinstance(identifier, list) else [identifier]):
                    if value.get('#text').startswith('http://ark.cdlib.org/ark:') \
                    and is_absolute(value.get('#text')):
                        ark = value.get('#text')
                        break

        calisphere_id = f"{self.collection_id}--{ark}"
        document['calisphere-id'] = calisphere_id
        return document
