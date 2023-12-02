import logging
import requests

from requests.adapters import HTTPAdapter, Retry
from rikolti.utils.versions import put_vernacular_page


logger = logging.getLogger(__name__)


class InvalidHarvestEndpoint(Exception):
    '''Raised when the harvest endpoint is invalid'''


class CollectionIdRequired(Exception):
    '''Raised when the collection id is invalid'''


class FetchError(Exception):
    pass


class Fetcher(object):
    def __init__(self, params: dict):
        """
        params: dict
            harvest_type: str
            collection_id: str or int
            write_page: str or int filename of the page to write to
            vernacular_version: path relative to collection id
                ex: "3433/vernacular_version_1"
        """
        self.harvest_type = params.get('harvest_type')
        self.collection_id = params.get('collection_id')
        self.write_page = params.get('write_page', 0)
        self.vernacular_version = params['vernacular_version']


        if not self.collection_id:
            raise CollectionIdRequired("collection_id is required")

    def fetch_page(self):
        """
        returns a dict with the following keys:
            document_count: int
            vernacular_filepath: path relative to collection id
                ex: "3433/vernacular_version_1/data/1"
            status: 'success' or 'error'
        """
        page = self.build_fetch_request()
        logger.debug(
            f"[{self.collection_id}]: fetching page {self.write_page} "
            f"at {page.get('url')}"
        )
        try:
            response = requests.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch page {page}")

        record_count = self.check_page(response)
        filepath = None
        if record_count:
            content = self.aggregate_vernacular_content(response.text)
            try:
                filepath = put_vernacular_page(
                    content, self.write_page, self.vernacular_version)
            except Exception as e:
                print(f"Metadata Fetcher: {e}")
                raise(e)

        self.increment(response)

        return {
            'document_count': record_count,
            'vernacular_filepath': filepath,
            'status': 'success'
        }

    def aggregate_vernacular_content(self, response):
        return response

    def build_fetch_request(self):
        """build parameters for the institution's requests.get()

        this should minimally return {'url': str} but may also include
        {'headers': {}, 'params': {}} or any other options accepted by
        https://docs.python-requests.org/en/latest/api/#requests.get
        """
        pass

    def get_records(self, http_resp):
        """parses http_resp from institutional API into a list of records

        should return a list of dictionaries which can easily be serialized
        by json.dumps into json line format; takes as an argument:
        https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        pass

    def increment(self, http_resp):
        """increment internal state for fetching the next page

        takes as an argument the http_resp from institution API call
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse
        """
        self.write_page = self.write_page + 1

    def json(self):
        """build json serialization of current state"""
        pass

    def make_http_request(self, url: str) -> requests.Response:
        """
        Given a URL, will return the response, retrying per the argument passed to
        Retry().

        Parameters:
            url: str

        Returns:
             requests.Response
        """
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session.get(url=url)

    def __str__(self):
        """build string representation of current state"""
        attrs = vars(self)
        return (', '.join("%s: %s" % item for item in attrs.items()))
