import json
import logging
from urllib.parse import quote as urllib_quote
from rikolti.utils.versions import put_vernacular_page

import requests

from .. import settings
from .Fetcher import Fetcher, InvalidHarvestEndpoint, FetchedPageStatus

logger = logging.getLogger(__name__)

# The NuxeoFetcher overwrites the main Fetcher method "fetch_page" (used by
# lambda_function.fetch_collection) and uses a different architecture to
# retrieve nuxeo pages.
#
# Most fetchers only need to paginate through a record list of unknown length,
# but the Nuxeo fetcher must paginate through a record list of unknown length,
# and then for each record, paginate through a component object list of
# unknown length, and then paginate through a folder list of unknown length
# and for each folder, paginate through a record list (and those record's
# component lists) - the folder structure can be arbitrarily deep.
#
# `.fetch_page` traverses this linear iteration (pagination of folder pages,
# record pages, and component pages), fan out (for each record, for each
# folder) and recursion (down a folder tree) in one function call and returns
# a list of pages, each of which includes a list of children. 
#
# `.json` returns {'finished': True} to prevent lambda_function.fetch_collection
# from trying to fetch the next page since pagination is already performed
# internally, here.

class NuxeoFetcher(Fetcher):
    nuxeo_request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-NXDocumentProperties": "*",
        "X-NXRepository": "default",
        "X-Authentication-Token": settings.NUXEO_TOKEN
    }

    def __init__(self, params: dict):
        super(NuxeoFetcher, self).__init__(params)
        # a root path is required for fetching from Nuxeo
        harvest_data = params.get('harvest_data', {})
        root_path = harvest_data.get('root_path', harvest_data.get('harvest_extra_data'))
        if not root_path:
            raise InvalidHarvestEndpoint(
                f"[{self.collection_id}]: please add a path to "
                "the harvest_extra_data field in the collection registry"
            )
        root_path = '/' + root_path.lstrip('/')

        # initialize default values for the fetcher
        self.nuxeo = {
            'root_path': root_path,
            'fetch_children': True,
        }
        self.nuxeo.update(harvest_data)

        # initialize current path with {path, uid} of root path
        if not self.nuxeo.get('current_path'):
            escaped_path = urllib_quote(root_path, safe=' /').strip('/')
            request = {
                'url': (
                    "https://nuxeo.cdlib.org/Nuxeo/site/api/"
                    f"v1/path/{escaped_path}"
                ),
                'headers': self.nuxeo_request_headers
            }
            try:
                response = requests.get(**request)
                response.raise_for_status()
            except Exception as e:
                print(
                    f"{self.collection_id:<6}: A path UID is required for "
                    f"fetching - could not retrieve root path uid: "
                    f"{request['url']}"
                )
                raise(e)
            self.nuxeo['current_path'] = {
                'path': root_path,
                'uid': response.json().get('uid')
            }

    def get_page_of_components(self, record: dict, component_page_count: int):
        recursive_object_nxql = (
            "SELECT * FROM SampleCustomPicture, CustomFile, "
            "CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{record['uid']}' "
            "AND ecm:isTrashed = 0 ORDER BY ecm:pos"
        )
        query = recursive_object_nxql

        # using the @search endpoint results in components being out of order
        # in the response for some objects
        request = {
            'url': "https://nuxeo.cdlib.org/Nuxeo/site/api/v1/search/lang/NXQL/execute",
            'headers': self.nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': component_page_count,
                'query': query
            }
        }

        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch components: {request}")
            raise(e)
        return response

    def get_pages_of_record_components(self, record: dict):
        """
        Components of a complex object record are served at 100 components per page
        Iterate through all pages of components of a single parent object, writing
        each page to Rikolti storage at:
            <vernacular_version>/children/<record uuid>-<page number>

        Returns a list of FetchedPageStatus objects for each page, for example:
            [
                FetchedPageStatus(document_count = 100, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page1'),
                FetchedPageStatus(document_count = 100, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page2'),
                FetchedPageStatus(document_count =   8, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page3'),
            ]
        to indicate that there are 208 child records to record 1 of 3433 saved on 3 different pages
        """
        pages_of_record_components = []
        component_page_count = 0
        more_component_pages = True
        while more_component_pages:
            component_resp = self.get_page_of_components(record, component_page_count)
            more_component_pages = component_resp.json().get('isNextPageAvailable')
            if not component_resp.json().get('entries', []):
                more_component_pages = False
                continue

            child_version_page = put_vernacular_page(
                component_resp.text,
                f"children/{record['uid']}-{component_page_count}",
                self.vernacular_version
            )
            pages_of_record_components.append(
                FetchedPageStatus(
                    len(component_resp.json().get('entries', [])),
                    child_version_page
                )
            )
            component_page_count+=1
        return pages_of_record_components

    def get_page_of_documents(self, folder: dict, record_page_count: int):
        # for some reason, using `ORDER BY ecm:name` in the query avoids
        # the bug where the API was returning duplicate records from Nuxeo
        parent_nxql = (
            "SELECT * FROM SampleCustomPicture, CustomFile, "
            "CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:parentId = '{folder['uid']}' AND "
            "ecm:isTrashed = 0 ORDER BY ecm:name"
        )
        query = parent_nxql

        request = {
            'url': "https://nuxeo.cdlib.org/Nuxeo/site/api/v1/path/@search",
            'headers': self.nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': record_page_count,
                'query': query
            }
        }

        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def get_pages_of_records(self, folder: dict, page_prefix: list):
        record_pages = []
        record_page_count = 0
        more_pages_of_records = True
        while more_pages_of_records:
            document_resp = self.get_page_of_documents(folder, record_page_count)
            more_pages_of_records = document_resp.json().get('isNextPageAvailable')
            if not document_resp.json().get('entries', []):
                more_pages_of_records = False
                continue

            version_page = put_vernacular_page(
                document_resp.text, 
                f"{'-'.join(page_prefix)}-p{record_page_count}", 
                self.vernacular_version
            )

            # pages of records components is a flat list of all children of all
            # records that were found on this page of records
            pages_of_records_components = []
            for record in document_resp.json().get('entries', []):
                pages_of_records_components.extend(
                    self.get_pages_of_record_components(record))

            record_pages.append(
                FetchedPageStatus(
                    len(document_resp.json().get('entries', [])),
                    version_page,
                    pages_of_records_components
                )
            )
            record_page_count += 1
        return record_pages

    def get_page_of_folders(self, folder: dict, folder_page_count: int):
        recursive_folder_nxql = (
            "SELECT * FROM Organization "
            f"WHERE ecm:path STARTSWITH '{folder['path']}' "
            "AND ecm:isTrashed = 0"
        )
        query = recursive_folder_nxql

        request = {
            'url': "https://nuxeo.cdlib.org/Nuxeo/site/api/v1/path/@search",
            'headers': self.nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': folder_page_count,
                'query': query
            }
        }

        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def folder_traversal(self, root_folder: dict, page_prefix: list):
        folder_page_count = 0
        more_pages_of_folders = True
        pages = []
        while more_pages_of_folders:
            folder_resp = self.get_page_of_folders(root_folder, folder_page_count)
            more_pages_of_folders = folder_resp.json().get('isNextPageAvailable')
            page_prefix.append(f"fp{folder_page_count}")

            for i, folder in enumerate(folder_resp.json().get('entries', [])):
                page_prefix.append(f"f{i}")
                pages.extend(self.get_pages_of_records(folder, page_prefix))
                page_prefix.pop()

            page_prefix.pop()
            folder_page_count += 1
        return pages

    def fetch_page(self) -> list[FetchedPageStatus]:
        page_prefix = ['r']
        # page_prefix is manipulated during tree traversal to indicate which
        # folder page, folder, and document page we're currently at.
        # `r` is root, `fp` is folder page, `f` is folder, and `p` is page

        # page_prefix is used as the filename for each page of vernacular 
        # metadata, so there is a trace back to the nuxeo location.

        # ex:

        # r-fp0-f0-p0 should be read as the first page of records inside the
        # first folder inside the first page of folders inside the root, or
        # root, folder page 0, folder 0, page 0

        # r-p0 is the first page of records inside the root directory, or
        # root page 0
        pages = self.get_pages_of_records(self.nuxeo['current_path'], page_prefix)
        pages.extend(self.folder_traversal(self.nuxeo['current_path'], page_prefix))
        return pages

    def json(self):
        return json.dumps({"finished": True})
