import json
import requests
from xml.etree import ElementTree
from Fetcher import Fetcher, FetchError
from urllib.parse import parse_qs
from sickle import Sickle

NS = {'oai2': 'http://www.openarchives.org/OAI/2.0/'}

# https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
class OAIFetcher(Fetcher):

    def __init__(self, params):
        super(OAIFetcher, self).__init__(params)

        self.oai = params.get('oai')

        oai_qs_args = self.oai.get('oai_qs_args')

        # set
        self.oai_set = self.oai.get('oai_set')
        if not self.oai_set:
            if oai_qs_args and 'set' in oai_qs_args:
                self.oai_set = self.get_value_from_qs(oai_qs_args, 'set')
            else:
                self.oai_set = oai_qs_args

        # metadataPrefix
        self.oai_metadata_prefix = self.oai.get('oai_metadata_prefix')
        if not self.oai_metadata_prefix and oai_qs_args:
            self.set_oai_md_prefix(oai_qs_args)

    def get_value_from_qs(self, query_string, key):
        ''' Parse query_string and return the value of key
            See https://docs.python.org/3/library/urllib.parse.html#urllib.parse.parse_qs
        '''
        if key in query_string:
            qs_dict = parse_qs(query_string)
            return qs_dict.get(key)[0]
        else:
            return None

    def set_oai_md_prefix(self, query_string):
        ''' Set metadata prefix for the OAI feed

            First check oai_qs_args
            Then, if `oai_qdc` is supported, use it
            Finally, use `oai_dc`
        '''
        if query_string:
            if 'metadataPrefix' in query_string:
                self.oai_metadata_prefix = self.get_value_from_qs(query_string, 'metadataPrefix')
                return

        sickle_client = Sickle(self.oai.get('url'))
        md_formats = [x for x in sickle_client.ListMetadataFormats()]
        for f in md_formats:
            if f.metadataPrefix == 'oai_qdc':
                self.oai_metadata_prefix = 'oai_qdc'
                return

        self.oai_metadata_prefix = 'oai_dc'

    def build_fetch_request(self):

        if self.oai.get('resumption_token'):
            url = (
                f"{self.oai.get('url')}"
                f"?verb=ListRecords"
                f"&resumptionToken={self.oai.get('resumption_token')}"
            )
        else:
            url = (
                f"{self.oai.get('url')}"
                f"?verb=ListRecords"
                f"&metadataPrefix={self.oai_metadata_prefix}"
                f"&set={self.oai_set}"
            )

        request = {"url": url}

        print(
            f"{self.collection_id}: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def check_page(self, http_resp):
        xml_resp = ElementTree.fromstring(http_resp.content)
        xml_hits = xml_resp.find('oai2:ListRecords', NS).findall('oai2:record', NS)

        if len(xml_hits) > 0:
            requested_url = (
                f"{self.oai.get('url')}"
                f"?verb=ListRecords&metadataPrefix={self.oai_metadata_prefix}"
                f"&set={self.oai_set}"
            )
            print(
                f"{self.collection_id}: Fetched page {self.write_page} "
                f"at {requested_url} "
                f"with {len(xml_hits)} hits"
            )
        return bool(len(xml_hits))

    def increment(self, http_resp):
        super(OAIFetcher, self).increment(http_resp)

        # if there is a resumption token, then increment
        xml_resp = ElementTree.fromstring(http_resp.content)
        resumption_token_node = xml_resp.find('oai2:ListRecords/oai2:resumptionToken', NS)

        if resumption_token_node is not None:
            self.oai['resumption_token'] = resumption_token_node.text
        else:
            self.oai['resumption_token'] = None

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

