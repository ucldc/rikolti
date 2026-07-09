import json
import logging
import requests
from urllib.parse import urlsplit, quote

from .Fetcher import Fetcher, FetchError

class PreservicaApiFetcher(Fetcher):
    BASE_URL ="https://us.preservica.com/api"

    def __init__(self, params):
        """
        Parameters:
            params: dict[str]
        """
        super(PreservicaApiFetcher, self).__init__(params)

        self.harvest_data = params.get("harvest_data", {})
        self.url = self.harvest_data.get("url").replace("http://","https://")

        self.access_token = params.get("access_token")
        if not self.access_token:
            self.access_token = self.get_access_token()

        self.preservica_collection_id = params.get("preservica_collection_id")
        if not self.preservica_collection_id:
            self.preservica_collection_id = self.get_preservica_collection_id()

        self.num_fetched = params.get("num_fetched", 0)
        self.start_at = params.get("start_at", 0)

    def build_fetch_request(self) -> dict[str]:
        # https://us.preservica.com/api/content/documentation.html#/%2F/get_object_children
        fields_to_fetch = [
            'id',
            'oai_dc.contributor',
            'oai_dc.coverage',
            'oai_dc.creator',
            'oai_dc.date',
            'oai_dc.description',
            'oai_dc.format',
            'oai_dc.identifier',
            'oai_dc.language',
            'oai_dc.publisher',
            'oai_dc.relation',
            'oai_dc.rights',
            'oai_dc.source',
            'oai_dc.subject',
            'oai_dc.title',
            'oai_dc.type'
        ]
        url = (
                f"{self.BASE_URL}/content/object-children?"
                f"id={quote(self.preservica_collection_id)}"
                f"&q={quote('{}')}"
                f"&start={self.start_at}"
                f"&max=100"
                f"&metadata={quote(',').join(fields_to_fetch)}"
        )

        headers = {
                "Preservica-Access-Token": self.access_token,
                "accept": "application/json"
            }

        request = {
                "url": url,
                "headers": headers
            }

        return request

    def get_preservica_collection_id(self) -> str:
        # example harvest url:
        # https://oakland.access.preservica.com/uncategorized/SO_46d67cb7-caad-4d3d-aec1-2fef7c7e7ae7/
        path = urlsplit(self.url).path.strip('/')
        id = path.split('/')[-1]
        if not id.startswith('SO_'):
            raise FetchError(
                    f"[{self.collection_id}]: invalid ID provided: {id}"
                    f"ID must start with 'SO_'"
                )

        return id.replace("SO_", "sdb:SO|")

    def get_access_token(self) -> str:
        """
        Get access token: https://us.preservica.com/api/accesstoken/documentation.html#//post_login
        """
        credentials = self.harvest_data.get("harvest_extra_data")
        credentials = [c.strip()for c in credentials.split(',')]
        user = credentials[0]
        password = credentials[1]
        url = f"{self.BASE_URL}/accesstoken/login"
        data = {
            "username": user,
            "password": password,
            "cookie": "false",
            "includeUserDetails": "false"
        }

        headers = {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        request = {
            "url": f"{self.BASE_URL}/accesstoken/login",
            "data": data,
            "headers": headers
        }
        try:
            response = self.http_session.post(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FetchError(
                f"[{self.collection_id}]: unable to get api access token from {url}",
                f"Error was: {e}"
            )

        token = response.json().get("token")
        if not token:
            raise FetchError(
                f"[{self.collection_id}]: unable to get api access token from {url}"
                f"Response was: {response}"
            )

        return token

    def check_page(self, http_resp) -> int:
        data = http_resp.json()
        items = data.get("value").get("metadata")
        if len(items) > 0:
            logging.debug(
                f"{self.collection_id}, fetched page {self.write_page} - "
                f"{len(items)} hits,-,-,-,-,-"
            )

        return len(items)

    def increment(self, http_resp):
        self.write_page = self.write_page + 1

        fetched_page = http_resp.json()
        self.num_fetched += len(fetched_page.get("value", {}).get("metadata", []))
        if self.num_fetched >= fetched_page.get("value", {}).get("totalHits"):
            self.finished = True
        else:
            self.finished = False

        self.start_at = self.num_fetched

    def json(self) -> str:
        return json.dumps({
            "finished": self.finished,
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "harvest_data": self.harvest_data,
            "write_page": self.write_page,
            "access_token": self.access_token,
            "preservica_collection_id": self.preservica_collection_id,
            "num_fetched": self.num_fetched,
            "start_at": self.start_at
        })
