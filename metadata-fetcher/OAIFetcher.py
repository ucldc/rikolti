import json
import xmltodict
from Fetcher import Fetcher, FetchError
from urllib.parse import parse_qs


class OAIFetcher(Fetcher):
    # params['oai'] = {
    #     "url": (
    #             "http://www.adc-exhibits.museum.ucsb.edu/"
    #             "oai-pmh-repository/request"),
    #     "extra_data": "metadataPrefix=oai_dpla&set=member_of_collection_ids_ssim:3vb00000zz-89112",
    #     "resumption_token": ""
    # }
    # params['oai'] = {
    #     "url": (
    #             "http://www.adc-exhibits.museum.ucsb.edu/"
    #             "oai-pmh-repository/request"),
    #     "extra_data": "big-pine-citizen-newspaper",
    #     "resumption_token": ""
    # }
    # params['oai'] = {
    #     "url": (
    #             "http://www.adc-exhibits.museum.ucsb.edu/"
    #             "oai-pmh-repository/request"),
    #     "resumption_token": ""
    # }
    # params['oai'] = {
    #     "url": (
    #             "http://www.adc-exhibits.museum.ucsb.edu/"
    #             "oai-pmh-repository/request"),
    #     "metadata_prefix": "oai_dpla",
    #     "metadata_set": "member_of_collection_ids_ssim:3vb00000zz-89112",
    #     "resumption_token": ""
    # }
    def __init__(self, params):
        super(OAIFetcher, self).__init__(params)
        self.oai = params.get('oai')

        # default metadata_prefix is oai_dc, default metadata_set is None
        if 'metadata_prefix' not in self.oai:
            self.oai['metadata_prefix'] = 'oai_dc'
        if 'metadata_set' not in self.oai:
            self.oai['metadata_set'] = None

        # metadata_prefix must be set explicitly in extra_data
        # metadata_set can be set implicitly or explicitly
        if 'extra_data' in self.oai:
            params = parse_qs(self.oai['extra_data'])
            if 'metadataPrefix' in params:
                self.oai['metadata_prefix'] = params['metadataPrefix'][0]
            if 'set' in params:
                self.oai['metadata_set'] = params['set'][0]
            else:
                self.oai['metadata_set'] = self.oai['extra_data']

    def build_fetch_request(self):
        url = self.oai.get('url')
        metadata_set = self.oai.get('metadata_set')
        metadata_prefix = self.oai.get('metadata_prefix')
        resumption_token = self.oai.get('resumption_token')

        if not resumption_token:
            request = {"url": (
                    f'{url}?verb=ListRecords'
                    f'&metadataPrefix={metadata_prefix}&set={metadata_set}'
                )}
        else:
            request = {"url": (
                f'{url}?verb=ListRecords'
                f'&resumptionToken={resumption_token}'
            )}

        print(
            f"{self.collection_id}: Fetching page "
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
            doc = record.get('metadata')
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
