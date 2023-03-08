import json
import settings
import boto3
import os
import requests
import derivatives
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter, Retry
from collections import Counter


class DownloadError(Exception):
    pass


def get_mapped_records(collection_id, page_filename) -> list:
    local_path = settings.local_path(
        'mapped_metadata', collection_id)
    page_path = os.sep.join([local_path, str(page_filename)])
    page = open(page_path, "r")
    mapped_records = json.loads(page.read())
    return mapped_records


def write_mapped_records(collection_id, page_filename, harvested_page):
    local_path = settings.local_path(
        'mapped_with_content', collection_id)
    page_path = os.sep.join([local_path, str(page_filename)])
    page = open(page_path, "w")
    page.write(json.dumps(harvested_page))


def get_child_records(collection_id, parent_id) -> list:
    local_path = settings.local_path('mapped_metadata', collection_id)
    children_path = os.sep.join([local_path, 'children'])

    mapped_child_records = []
    if os.path.exists(children_path):
        child_pages = [file for file in os.listdir(children_path)
                       if file.startswith(parent_id)]
        for child_page in child_pages:
            child_page_path = os.sep.join([children_path, child_page])
            page = open(child_page_path, "r")
            mapped_child_records.extend(json.loads(page.read()))
    return mapped_child_records


class ContentHarvester(object):

    # context = {'collection_id': '12345', 'page_filename': '1.jsonl'}
    def __init__(self, context=None, src_auth=None):
        self.http = requests.Session()

        retry_strategy = Retry(
            total=3,
            status_forcelist=[413, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

        self.src_auth = src_auth
        self.harvest_context = context

    # returns media_dest = {media_filepath, mimetype}
    def harvest_media(self, calisphere_id, media_src, media_src_file) -> dict:
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')
        media_dest = None

        if media_src_file:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"Media Source: {media_src}, Temp File: {media_src_file}, "
                f"fsize: {os.path.getsize(media_src_file)}"
            )
            if media_src.get('nuxeo_type') == 'SampleCustomPicture':
                media_dest = derivatives.make_jp2(
                    media_src_file, media_src.get('mimetype'))
            else:
                media_dest = {
                    'media_filepath': media_src_file,
                    'mimetype': media_src.get('mimetype')
                }
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}] "
                f"Media Path: {media_dest}"
            )
        return media_dest

    # returns thumbnail_dest = {thumbnail_filepath, mimetype}
    def harvest_thumbnail(
            self, calisphere_id, thumbnail_src, thumbnail_src_file) -> dict:
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')
        thumbnail_dest = None

        if thumbnail_src_file:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"Thumb Source: {thumbnail_src}, Temp File: "
                f"{thumbnail_src_file}, fsize: "
                f"{os.path.getsize(thumbnail_src_file)}"
            )
            thumbnail_dest = derivatives.make_thumbnail(
                thumbnail_src_file,
                thumbnail_src.get('mimetype')
            )
        print(
            f"[{collection_id}, {page_filename}, {calisphere_id}] "
            f"Thumbnail Path: {thumbnail_dest}"
        )
        return thumbnail_dest

    # returns content = {thumbnail, media, children} where children
    # is an array of the self-same content dictionary
    def harvest(self, record) -> dict:
        calisphere_id = record.get('calisphere-id')
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')

        # Harvest Media File for Record
        media_src = record.get('media_source')
        media_src_file = self._download_src(media_src)
        media_dest = self.harvest_media(
            calisphere_id, media_src, media_src_file)

        # Harvest Thumbnail File for Record
        thumbnail_src = record.get('thumbnail_source')
        thumbnail_src_file = self._download_src(thumbnail_src)
        thumbnail_dest = self.harvest_thumbnail(
            calisphere_id, thumbnail_src, thumbnail_src_file)
        
        if media_src_file:
            os.remove(media_src_file)
        if thumbnail_src_file:
            os.remove(thumbnail_src_file)

        # Recurse through the record's children (if any)
        child_records = get_child_records(collection_id, calisphere_id)

        print(
            f"[{collection_id}, {page_filename}, {calisphere_id}]: "
            f"{len(child_records)} children found."
        )

        children_media = []
        for child in child_records:
            children_media.append(self.harvest(child))

        content = {
            'thumbnail': thumbnail_dest,
        }
        if media_dest:
            content['media'] = media_dest
        if children_media:
            content['children'] = children_media
        return content

    def _download_src(self, content_src) -> str:
        '''
            download source file to local disk
        '''
        if not content_src:
            return None

        filename = content_src.get('filename')
        src_url = content_src.get('url')

        tmp_file_path = os.path.join('/tmp', filename)
        if os.path.exists(tmp_file_path):
            return tmp_file_path

        # Weird how we have to use username/pass to hit this endpoint
        # but we have to use auth token to hit API endpoint
        request = {
            "url": src_url,
            "stream": True,
            "timeout": (12.05, (60 * 10) + 0.05) # connect, read
        }
        if self.src_auth:
            request['auth'] = self.src_auth

        try:
            response = self.http.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise DownloadError(
                f"ERROR: failed to download source file: {err}"
            )

        with open(tmp_file_path, 'wb') as f:
            for block in response.iter_content(1024):
                f.write(block)

        return tmp_file_path


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo", "page_filename": "r-0"}
def harvest_page_content(collection_id, rikolti_mapper_type, page_filename, **kwargs):

    records = get_mapped_records(collection_id, page_filename)
    print(
        f"[{collection_id}, {page_filename}]: "
        f"Harvesting content for {len(records)}"
    )

    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    harvester = ContentHarvester(context={
        'collection_id': collection_id, 
        'page_filename': page_filename
    }, src_auth=auth)

    for record in records:
        print(
            f"[{collection_id}, {page_filename}]: "
            f"Harvesting record {record.get('calisphere-id')}"
        )

        content = harvester.harvest(record)

        if not content.get('thumbnail'):
            warn_level = "ERROR"
            if 'sound' in record.get('type', []):
                warn_level = "WARNING"
            print(
                f"[{collection_id}, {page_filename}]: "
                f"{record.get('calisphere-id')}: {warn_level} - "
                f"NO THUMBNAIL: {record.get('type')}"
            )

        record['content'] = content

    write_mapped_records(collection_id, page_filename, records)

    # reporting aggregate stats
    media_mimetypes = [record.get('content', {}).get('media', {}).get('mimetype') for record in records]
    thumb_mimetypes = [record.get('content', {}).get('thumbnail', {}).get('mimetype') for record in records]
    media_source_mimetype = [record.get('media_source', {}).get('mimetype') for record in records]
    thumb_source_mimetype = [record.get('thumbnail_source', {}).get('mimetype') for record in records]
    child_contents = [len(record.get('content', {}).get('children', [])) for record in records]
    
    return {
        'thumb_source': Counter(thumb_source_mimetype),
        'thumb_mimetypes': Counter(thumb_mimetypes),
        'media_source': Counter(media_source_mimetype),
        'media_mimetypes': Counter(media_mimetypes),
        'children': sum(child_contents),
        'records': len(records)
    }
