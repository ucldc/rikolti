import json
from xml.etree import ElementTree
from Fetcher import Fetcher
from urllib.parse import parse_qs
from sickle import Sickle

NAMESPACE = {'oai2': 'http://www.openarchives.org/OAI/2.0/'}


# https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
class OAIFetcher(Fetcher):

    def __init__(self, params):
        super(OAIFetcher, self).__init__(params)

        self.oai = params.get('oai')

        if self.oai.get('query_params'):
            # see if we have a query string, e.g. "metadataPrefix=marcxml&set=fritz-metcalf"
            parsed_params = {k: v[0] for k, v in parse_qs(self.oai.get('query_params')).items()}
            self.metadata_prefix = parsed_params.get('metadataPrefix')
            self.metadata_set = parsed_params.get('set')

            # if not, then assume we just have a string value for set, e.g. "big-pine-citizen-newspaper"
            if not parsed_params:
                self.metadata_set = self.oai.get('query_params')
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
                f"&metadataPrefix={self.metadata_prefix}"
                f"&set={self.metadata_set}"
            )

        request = {"url": url}

        print(
            f"{self.collection_id}: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def check_page(self, http_resp):
        xml_resp = ElementTree.fromstring(http_resp.content)
        xml_hits = xml_resp.find('oai2:ListRecords', NAMESPACE).findall('oai2:record', NAMESPACE)

        if len(xml_hits) > 0:
            requested_url = (
                f"{self.oai.get('url')}"
                f"?verb=ListRecords&metadataPrefix={self.metadata_prefix}"
                f"&set={self.metadata_set}"
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
        resumption_token_node = xml_resp.find('oai2:ListRecords/oai2:resumptionToken', NAMESPACE)

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

