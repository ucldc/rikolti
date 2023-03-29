import json
from .Fetcher import Fetcher
import requests
from urllib.parse import urlencode
import settings

class FlkFetcher(Fetcher):
    BASE_URL: str = "https://api.flickr.com/services/rest/"

    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(FlkFetcher, self).__init__(params)

        # Tracks where we're at processing photo requests
        self.photo_index = 1
        self.photo_total = 0

        # If `next_url` is a param, we know that this is not the fetch of the
        # first page, so skip setting those attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        self.collection_id = params.get("collection_id")
        self.harvest_extra_data = params.get("harvest_data").\
            get("harvest_extra_data")
        self.per_page = 100
        self.next_url = self.get_current_url()

    def get_current_url(self) -> str:
        """
        Returns the request URL based on the type of collection, photoset or
        user_photos. Note we increment the page number since Flickr starts
        with page 1 and internal page iteration starts with page 0.

        Returns: str
        """
        if self.is_photoset():
            return self.get_photoset_request_url(self.harvest_extra_data,
                                                 self.write_page + 1)
        return self.get_user_photos_request_url(self.harvest_extra_data,
                                                self.write_page + 1)

    def is_photoset(self) -> bool:
        """
        Based on the harvest_extra_data, determines if it's a photoset vs user
        photos.

        Returns: bool
        """
        return "@N" not in self.harvest_extra_data

    def get_user_photos_request_url(self, user_id: str, page: int) -> str:
        """
        Generates `flickr.people.getPublicPhotos` request URL.

        Parameters:
            user_id: str
            page: int

        Returns: str
        """
        params = self.get_request_method_params("flickr.people.getPublicPhotos")
        params.update({
            "user_id": user_id,
            "per_page": self.per_page,
            "page": page
        })
        return self.build_request_url(params)

    def get_photoset_request_url(self, photoset_id: str, page: int) -> str:
        """
        Generates `flickr.photosets.getPhotos` request URL.

        Parameters:
            photoset_id: str
            page: int

        Returns: str
        """
        params = self.get_request_method_params("flickr.photosets.getPhotos")
        params.update({
            "photoset_id": photoset_id,
            "per_page": self.per_page,
            "page": page
        })
        return self.build_request_url(params)

    def get_photo_info_request_url(self, photo_id: str) -> str:
        """
        Generates `flickr.photos.getInfo` request URL.

        Parameters:
            photo_id: str

        Returns: str
        """
        params = self.get_request_method_params("flickr.photos.getInfo")
        params.update({
            "photo_id": photo_id
        })
        return self.build_request_url(params)

    def get_request_method_params(self, method: str) -> dict[str]:
        """
        Generates the base set of parameters for all Flickr API requests

        Parameters:
            method: str

        Returns: dict[str]
        """
        return {
            "api_key": settings.FLICKR_API_KEY,
            "method": method
        }

    def build_request_url(self, params: dict[str]) -> str:
        """
        Creates a Flickr API request URL from dictionary of request parameters.

        Parameters:
            params: dict[str]

        Returns: str
        """
        return self.BASE_URL + "?" + urlencode(params)

    def build_fetch_request(self) -> dict[str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        request = {"url": self.get_current_url()}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def get_text_from_response(self, response: requests.Response) -> str:
        """
        Accepts a response from a request for page of photos, and transforms it
        in a dictionary. This requiresa `flickr.photos.getInfo` request for each
         photo.

        Parameters:
            response: requests.Response

        Returns: str
        """
        set_element_tree = ElementTree.fromstring(response.content)
        photos = set_element_tree.findall(".//photo")

        photo_data = []
        for photo in photos:
            content = self.get_photo_metadata(photo.attrib["id"]).content
            photo_data.append(self.flickr_photo_info_xml_to_dict(content))
            self.photo_index += 1

        print(
            f"[{self.collection_id}]: Fetched all photos for page"
            f" {self.write_page}"
        )

        return json.dumps(photo_data)

    @staticmethod
    def flickr_photo_info_xml_to_dict(xml: str) -> dict[Union[dict, list]]:
        """
        Transforms a raw Flickr XML response for `flickr.photos.getInfo` request
        into a dictionary. Tag attributes and text node are combined,
        with the text node included like this:

        {"attribute1": "value1", "attribute2": "value2", "text": "look at
        meeeeee!"}

        Several tags in the response are given special treatment because there
        can be multiple: `tags`, `notes`, `urls`.

        Parameters:
            xml: str

        Returns: dict[dict]
            The dict-ized
        """

        def node_to_dict(node: ElementTree.Element) -> dict:
            """
            Transforms an ElementTree.Element into a dictionary.

            Parameters:
                node: ElementTree.Element

            Returns: dict
            """
            node_dict = node.attrib
            node_text = node.text.strip() if node.text and\
                node.text.strip() else None
            if node_text:
                node_dict['text'] = node_text

            return node_dict

        photo_dict = {}
        tree = ElementTree.fromstring(xml)
        photo_node = tree.find(".//photo")
        photo_dict.update(photo_node.attrib)

        for photo_child_node in photo_node:
            if photo_child_node.tag in ("tags", "notes", "urls"):
                node_value = [node_to_dict(node_dict) for node_dict
                              in photo_child_node.iter()
                              if node_dict.items()]
            else:
                node_value = node_to_dict(photo_child_node)

            photo_dict[photo_child_node.tag] = node_value

        return photo_dict

    def get_photo_metadata(self, id: str) -> requests.Response:
        """
        Performs a request for photo info and returns the response.

        Parameters:
            id: str

        Returns: requests.Response
        """
        print(
            f"[{self.collection_id}]: Fetching photo {id} "
            f"({self.photo_index} of {self.photo_total})"
        )

        return requests.get(url=self.get_photo_info_request_url(id))

    def check_page(self, http_resp: requests.Response) -> bool:
        """
        Parameters:
            http_resp: requests.Response

        Returns: bool
        """
        element_tree = ElementTree.fromstring(http_resp.content)
        pagination = element_tree.findall(".//photo")
        self.photo_total = len(pagination)

        print(
            f"[{self.collection_id}]: Fetched page {self.write_page} "
            f"at {http_resp.url} with {self.photo_total} hits"
        )

        return True

    def increment(self, http_resp: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
             http_resp: requests.Response
        """
        super(FlkFetcher, self).increment(http_resp)

        tag = "photoset" if self.is_photoset() else "photos"

        element_tree = ElementTree.fromstring(http_resp.content)
        pagination = element_tree.find(tag)
        page = int(pagination.attrib['page'])
        pages = int(pagination.attrib['pages'])

        self.next_url = self.get_current_url() if page < pages else None

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "harvest_extra_data": self.harvest_extra_data,
            "collection_id": self.collection_id,
            "next_url": self.next_url,
            "write_page": self.write_page,
            "per_page": self.per_page
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
