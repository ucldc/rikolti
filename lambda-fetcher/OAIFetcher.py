import asyncio
import xmltodict
import json
from Fetcher import Fetcher

class OAIFetcher(Fetcher):
    def __init__(self, params):
        super(OAIFetcher, self).__init__(params)
        self.oai = params.get('oai')

    async def build_fetch_request(self):
        try:
            base = f"{self.oai.get('url')}?verb=ListRecords"
        except KeyError:
            print('no oai feed url')

        if not self.oai.get('resumptionToken'):
            # first request - url includes metadataPrefix and set
            url = base
            if self.oai.get('metadataPrefix'):
                url = f"{url}&metadataPrefix={self.oai.get('metadataPrefix')}"
            if self.oai.get('oai_set'):
                url = f"{url}&set={self.oai.get('oai_set')}"
        elif self.oai.get('resumptionToken').get('#text'):
            # next requests - url includes only resumptionToken
            url = f"{base}&resumptionToken={self.oai.get('resumptionToken').get('#text')}"
        else:
            print("No more pages to fetch")
            return None

        print(f"Fetching page {self.write_page} at {url}")
        return {"url": url}

    async def get_records(self, httpResp):
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        records = data.get('OAI-PMH',
            {}).get('ListRecords',
            {}).get('record')

        return records

    async def increment(self, httpResp):
        await super(OAIFetcher, self).increment()
        resp = await httpResp.text()
        data = xmltodict.parse(resp)

        self.oai['resumptionToken'] = data.get('OAI-PMH',
            {}).get('ListRecords',
            {}).get('resumptionToken', None)

        if not self.oai.get('resumptionToken'):
            return None

    async def json(self):
        return json.dumps({
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page, 
            "oai": self.oai
        })

