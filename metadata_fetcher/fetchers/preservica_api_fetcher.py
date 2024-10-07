import json
from xml.etree import ElementTree
from .Fetcher import Fetcher
from requests.auth import HTTPBasicAuth
from requests.adapters import Retry
from requests.adapters import HTTPAdapter
import requests
import re


class PreservicaApiFetcher(Fetcher):
    BASE_URL: str = "https://us.preservica.com/api/entity/v6.0"

    NAMESPACES: dict = {
        "pra": "http://preservica.com/EntityAPI/v6.0",
        "xip": "http://preservica.com/XIP/v6.0"
    }

    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(PreservicaApiFetcher, self).__init__(params)

        # Tracks where we're at processing record sub-requests
        self.record_index = 1
        self.record_total = 0

        # If `next_url` is a param, we know that this is not
        # the fetch of the first page, so skip setting those
        # attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        credentials = params.get("harvest_data").get("harvest_extra_data")
        self.basic_auth_credentials = ([v.strip() for v in credentials.split(',')])
        self.internal_collection_id = \
            re.search(
                r"(?<=SO_)[0-9a-f-]+",
                params.get("harvest_data").get("url")
            ).group(0)

        self.next_url = (
            f"{self.BASE_URL}/structural-objects/"
            f"{self.internal_collection_id}/children?max=1"
        )

    def build_fetch_request(self) -> dict[str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        return self.build_url_request(self.next_url)

    def aggregate_vernacular_content(self, response: str) -> str:
        """
        TODO: at time of dev, response is a requests.Response, but will be str when
        merged

        Parameters:
            response: str

        Returns: str
        """

        response_body = response.text

        # Starting with a list of `information-objects` URLs
        object_url = ElementTree.fromstring(response_body).\
            find("pra:Children/pra:Child", self.NAMESPACES).text

        # Getting an individual `information-object`, extracting the URL
        # for the oai_dc metadata fragment
        metadata_url = self.get_metadata_url_from_object(object_url)

        self.record_total = 1 if metadata_url else 0

        # Getting the metadata
        return self.get_metadata_from_url(metadata_url)

    def get_object_type(self, response_body: str):
        io_tag = ElementTree.fromstring(response_body).\
            find("xip:InformationObject", self.NAMESPACES)

        if io_tag:
            return "information"

        so_tag = ElementTree.fromstring(response_body).\
            find("xip:StructuralObject", self.NAMESPACES)

        if so_tag:
            return "structural"

    def get_information_response(self, response_body):
        """
        Takes an information object response and performs two additional requests
        to get to the structural object. This function is optimistic about the
        structure of the XML responses.

        Parameters:
            response_body: string
        Returns: string
        """
        object_type = self.get_object_type(response_body)

        if object_type == "information":
            return response_body

        path = "pra:AdditionalInformation/pra:Children"
        url = ElementTree.fromstring(response_body).find(path, self.NAMESPACES).text

        request = self.build_url_request(url)
        response_body = self.build_retry_session().get(**request).text

        path = "pra:Children/pra:Child"
        url = ElementTree.fromstring(response_body).find(path, self.NAMESPACES).text

        request = self.build_url_request(url)
        return self.build_retry_session().get(**request).text

    def get_metadata_url_from_object(self, url: str):
        """
        Second request. It may get a structural object or a information object. With
        the former, a couple additional requests have to take place.

        Parameters:
            url: str

        Returns: str
        """
        request = self.build_url_request(url)
        response_body = self.build_retry_session().get(**request).text

        if self.get_object_type(response_body) == "structural":
            response_body = self.get_information_response(response_body)

        root = ElementTree.fromstring(response_body)

        path = ".//pra:Fragment[@schema='http://www.openarchives.org/OAI/2.0/oai_dc/']"
        fragment = root.find(path, self.NAMESPACES)

        return fragment.text if fragment is not None else None

    def get_metadata_from_url(self, url: str) -> str:
        """
        Final request. It returns the metadata.

        Parameters:
            url: str

        Returns: str
        """
        print(
            f"[{self.collection_id}]: Fetching record "
            f"({self.record_index} of {self.record_total}) at {url}"
        )

        self.record_index += 1

        request = self.build_url_request(url)
        response = self.build_retry_session().get(**request)

        return response.text

    def check_page(self, response: requests.Response) -> int:
        """
        Parameters:
            response: requests.Response

        Return: int
        """
        hits = len(ElementTree.fromstring(response.content).
                   findall(".//pra:Child", self.NAMESPACES))

        print(
            f"[{self.collection_id}]: Fetched page {self.write_page} "
            f"at {response.url} with {hits} hits"
        )

        return hits

    def increment(self, response: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
            response: requests.Response
        """
        super(PreservicaApiFetcher, self).increment(response)

        next_element = ElementTree.fromstring(response.content).\
            find("pra:Paging/pra:Next", self.NAMESPACES)
        self.next_url = next_element.text if next_element is not None else None

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "basic_auth_credentials": self.basic_auth_credentials,
            "collection_id": self.collection_id,
            "internal_collection_id": self.internal_collection_id,
            "next_url": self.next_url,
            "write_page": self.write_page
        }
        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)

    def build_url_request(self, url: str) -> dict:
        """
        Creates a dictionary of parameters needed for requests for this fetcher

        Parameters:
            url: str

        Returns: dict
        """
        return {"url": url, "auth": HTTPBasicAuth(*self.basic_auth_credentials)}

    @staticmethod
    def build_retry_session() -> requests.Session:
        """
        Creates and returns request sessions that will retry requests

        Requests: requests.Session
        """
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session
