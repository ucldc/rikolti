import json
from xml.etree import ElementTree
from .Fetcher import Fetcher
from urllib.parse import parse_qs
from sickle import Sickle
import requests

NAMESPACE = {'oai2': 'http://www.openarchives.org/OAI/2.0/'}


# https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
class OaiFetcher(Fetcher):

    def __init__(self, params):
        super(OaiFetcher, self).__init__(params)

        self.oai = params.get('harvest_data')

        if self.oai.get('harvest_extra_data'):
            # see if we have a query string,
            # e.g. "metadataPrefix=marcxml&set=fritz-metcalf"
            parsed_params = {
                k: v[0]
                for k, v in parse_qs(self.oai.get('harvest_extra_data')).items()
            }
            self.metadata_prefix = parsed_params.get('metadataPrefix')
            self.metadata_set = parsed_params.get('set')

            # if not, then assume we just have a string value for set,
            # e.g. "big-pine-citizen-newspaper"
            if not parsed_params:
                self.metadata_set = self.oai.get('harvest_extra_data')
        else:
            self.metadata_prefix = self.oai.get('metadata_prefix')
            self.metadata_set = self.oai.get('metadata_set')

        if not self.metadata_prefix:
            self.metadata_prefix = self.get_md_prefix_from_feed()

    def get_md_prefix_from_feed(self):
        ''' check vernacular metadata to see which metadata formats are supported

            if `oai_qdc` is supported, use it; otherwise use `oai_dc`
        '''
        sickle_client = Sickle(self.oai.get('url'))
        md_formats = [x for x in sickle_client.ListMetadataFormats()]
        for f in md_formats:
            if f.metadataPrefix == 'oai_qdc':
                return 'oai_qdc'

        return 'oai_dc'

    def build_fetch_request(self):

        url = f"{self.oai['url']}?verb=ListRecords"
        if self.oai.get('resumption_token'):
            url += f"&resumptionToken={self.oai.get('resumption_token')}"
        else:
            url += f"&metadataPrefix={self.metadata_prefix}"
            if self.metadata_set:
                url += f"&set={self.metadata_set}"

        request = {"url": url}
        return request

    def check_page(self, http_resp: requests.Response) -> int:
        xml_resp = ElementTree.fromstring(http_resp.content)
        xml_hits = xml_resp.find(
            'oai2:ListRecords', NAMESPACE).findall('oai2:record', NAMESPACE)

        if len(xml_hits) > 0:
            print(
                f"[{self.collection_id}]: Fetched page {self.write_page}; "
                f"{len(xml_hits)} hits; {self.build_fetch_request()['url']}"
            )
        return len(xml_hits)

    def increment(self, http_resp):
        super(OaiFetcher, self).increment(http_resp)

        # if there is a resumption token, then increment
        xml_resp = ElementTree.fromstring(http_resp.content)
        resumption_token_node = xml_resp.find(
            'oai2:ListRecords/oai2:resumptionToken', NAMESPACE)

        if resumption_token_node is not None:
            self.oai['resumption_token'] = resumption_token_node.text
        else:
            self.oai['resumption_token'] = None

        return

    def json(self):
        current_state = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "harvest_data": self.oai
        }
        if not self.oai.get('resumption_token'):
            current_state.update({"finished": True})

        return json.dumps(current_state)
