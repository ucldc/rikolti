import json
import logging
from urllib.parse import quote as urllib_quote
from rikolti.utils.versions import put_versioned_page

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
# To complicate things even further, the Nuxeo database schema does not 
# support paged fetching of all descendants of a given root document recursively;
# it only supports paged fetching of direct children of a given document.
# Therefore, we need to implement recursion here in the client code.
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
    nuxeo_request_headers = {'Content-Type': 'application/json'}
    nuxeo_request_cookies = {'dbquerytoken': settings.NUXEO_TOKEN}

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
            payload = {
                'path': escaped_path,
                'relation': 'self',
                'results_type': 'full'
            }

            request = {
                'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
                'headers': self.nuxeo_request_headers,
                'cookies': self.nuxeo_request_cookies,
                'data': json.dumps(payload)
            }
            try:
                response = self.http_session.get(**request)
                response.raise_for_status()
            except Exception as e:
                print(
                    f"{self.collection_id:<6}: A path UID is required for "
                    f"fetching - could not retrieve root path uid for: "
                    f"{payload['path']}"
                )
                raise(e)
            self.nuxeo['current_path'] = {
                'path': root_path,
                'uid': response.json().get('uid')
            }

    def get_pages_of_record_components(self, root_record:dict):
        """
        Components of a complex object record are served at 100 components per page
        Iterate through all pages of components of a single parent object, writing
        each page to Rikolti storage at:
            <vernacular_version>/children/<record uuid>-<page number>

        It is possible for components to be nested inside components; in the case
        of multiple layers, the hierarchy is ignored and all layers of components
        are flattened into one.

        Returns a list of FetchedPageStatus objects for each page, for example:
            [
                FetchedPageStatus(document_count = 100, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page1'),
                FetchedPageStatus(document_count = 100, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page2'),
                FetchedPageStatus(document_count =   8, vernacular_filepath = '3433/vernacular_metadata_v1/data/children/record1-page3'),
            ]
        to indicate that there are 208 child records to record 1 of 3433 saved on 3 different pages
        """
        pages_of_record_components = []

        def recurse(pages):
            pages_of_record_components.extend(pages)
            for page in pages:
                records = page.get('entries', [])
                for record in records:
                    child_component_pages = self.get_pages_of_child_components(record)
                    recurse(child_component_pages)

        # get components of root record
        root_component_pages = self.get_pages_of_child_components(root_record)

        # recurse to fetch any nested components
        recurse(root_component_pages)

        component_page_count = 0
        pages_of_fetch_statuses = []
        for page in pages_of_record_components:
            child_version_page = put_versioned_page(
                json.dumps(page),
                f"children/{root_record['uid']}-{component_page_count}",
                self.vernacular_version
            )

            pages_of_fetch_statuses.append(
                FetchedPageStatus(
                    len(page.get('entries', [])),
                    child_version_page
                )
            )
            component_page_count += 1

        return pages_of_fetch_statuses

    def get_pages_of_child_components(self, record:dict):
        pages = []
        more_component_pages = True
        resume_after = ''

        while more_component_pages:
            component_resp = self.get_page_of_documents(record, resume_after)
            more_component_pages = component_resp.json().get('isNextPageAvailable')
            if not component_resp.json().get('entries', []):
                more_component_pages = False
                continue
            resume_after = component_resp.json().get('resumeAfter')

            pages.append(component_resp.json())

        return pages

    def get_page_of_documents(self, parent: dict, resume_after: str):
        payload = {
            'uid': parent['uid'],
            'doc_type': 'records',
            'results_type': 'full',
            'resume_after': resume_after
        }

        request = {
            'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
            'headers': self.nuxeo_request_headers,
            'cookies': self.nuxeo_request_cookies,
            'data': json.dumps(payload)
        }

        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def get_pages_of_records(self, folder: dict, page_prefix: list):
        record_pages = []
        record_page_count = 0
        more_pages_of_records = True
        resume_after = ''
        while more_pages_of_records:
            document_resp = self.get_page_of_documents(folder, resume_after)
            more_pages_of_records = document_resp.json().get('isNextPageAvailable')
            if not document_resp.json().get('entries', []):
                more_pages_of_records = False
                continue
            resume_after = document_resp.json().get('resumeAfter')

            version_page = put_versioned_page(
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

    def get_page_of_folders(self, folder: dict, resume_after: str):
        payload = {
            'uid': folder['uid'],
            'doc_type': 'folders',
            'results_type': 'full',
            'resume_after': resume_after
        }

        request = {
            'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
            'headers': self.nuxeo_request_headers,
            'cookies': self.nuxeo_request_cookies,
            'data': json.dumps(payload)
        }
        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def folder_traversal(self, root_folder: dict, page_prefix: list):
        pages = []

        collection_folders = self.get_descendant_folders(root_folder)

        for i, folder in enumerate(collection_folders):
            page_prefix.append(f"f{i}")
            print(f"{'-'.join(page_prefix)} {folder['path']}")
            pages.extend(self.get_pages_of_records(folder, page_prefix))
            page_prefix.pop()

        return pages

    def get_descendant_folders(self, root_folder):
        descendant_folders = []

        def recurse(folders):
            descendant_folders.extend(folders)
            for folder in folders:
                subfolders = self.get_child_folders(folder)
                recurse(subfolders)

        # get child folders of root
        root_folders = self.get_child_folders(root_folder)

        # recurse down the tree
        recurse(root_folders)

        return descendant_folders

    def get_child_folders(self, folder):
        subfolders = []
        more_pages_of_folders = True
        resume_after = ''

        while more_pages_of_folders:
            folder_resp = self.get_page_of_folders(folder, resume_after)
            more_pages_of_folders = folder_resp.json().get('isNextPageAvailable')
            resume_after = folder_resp.json().get('resumeAfter')

            for child_folder in folder_resp.json().get('entries', []):
                subfolders.append(child_folder)

        return subfolders

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
