import json
import xmltodict
from Fetcher import Fetcher, FetchError


class OAIFetcher(Fetcher):
    # params['oai'] = {
    #     "url": (
    #             "http://www.adc-exhibits.museum.ucsb.edu/"
    #             "oai-pmh-repository/request"),
    #     "set": "38",
    #     "resumption_token": ""
    # }
    def __init__(self, params):
        super(OAIFetcher, self).__init__(params)
        self.oai = params.get('oai')

    def build_fetch_request(self):
        url = self.oai.get('url')
        oai_set = self.oai.get('set')
        resumption_token = self.oai.get('resumption_token')

        if not resumption_token:
            request = {"url": (
                    f'{url}?verb=ListRecords'
                    f'&metadataPrefix=oai_dc&set={oai_set}'
                )}
        else:
            request = {"url": (
                f'{url}?verb=ListRecords'
                f'&resumptionToken={resumption_token}'
            )}

        print(
            f"Fetching page "
            f"at {request.get('url')}")
        return request

    def get_records(self, http_resp):
        response = xmltodict.parse(http_resp.text)
        records = (response
                   .get("OAI-PMH")
                   .get("ListRecords")
                   .get("record"))

        documents = []
        for record in records:
            doc = record.get('metadata').get('oai_dc:dc')
            doc.update({
                "calisphere-id": self.build_id(record)
            })
            documents.append(doc)

        return documents

    def build_id(self, document):
        oai_id = document.get('header').get('identifier')
        return f"{self.collection_id}--{oai_id}"

    def increment(self, http_resp):
        super(OAIFetcher, self).increment(http_resp)

        response = xmltodict.parse(http_resp.text)
        try:
            resumption_token = (response
                                .get('OAI-PMH')
                                .get('ListRecords')
                                .get('resumptionToken')
                                .get('#text'))
        except AttributeError:
            resumption_token = None

        self.oai['resumption_token'] = resumption_token
        return

    def json(self):
        if not self.oai.get('resumption_token'):
            return None

        return json.dumps({
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "oai": self.oai
        })
