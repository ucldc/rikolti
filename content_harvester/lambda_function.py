import json
import settings
import boto3
import os
import requests
import derivatives
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter, Retry


class DownloadError(Exception):
    pass


def get_mapped_records(collection_id, page_filename) -> dict:
    if settings.DATA_SRC == 'local':
        print(
            f"[{collection_id}, {page_filename}]: Getting local mapped records"
        )
        local_path = settings.local_path(
            'mapped_metadata', collection_id)
        page_path = os.sep.join([local_path, str(page_filename)])
        page = open(page_path, "r")
        mapped_page = json.loads(page.read())
        return mapped_page
    else:
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{collection_id}/{page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        mapped_page = json.loads(s3_obj_summary['Body'].read())
        return mapped_page


def write_mapped_records(collection_id, page_filename, harvested_page):
    if settings.DATA_SRC == 'local':
        local_path = settings.local_path(
            'mapped_with_content', collection_id)
        page_path = os.sep.join([local_path, str(page_filename)])
        page = open(page_path, "w")
        page.write(json.dumps(harvested_page))
    else:
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"mapped_with_content/{collection_id}/{page_filename}"
        s3.Object(bucket, key).put(json.dumps(harvested_page))


def get_child_records(collection_id, parent_id):
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
    if len(mapped_child_records) == 0:
        print(f"[{collection_id}, {parent_id}] NO CHILDREN")
    else:
        print(f"[{collection_id}, {parent_id}] YO CHILDREN")
    return mapped_child_records


class ContentHarvester(object):
    # context = {
    #   'mapper_type': 'nuxeo.nuxeo', 
    #   'collection_id': '12345', 
    #   'page_filename': '1.jsonl'
    # }
    def __init__(self, context, src_auth=None):
        self.http = requests.Session()

        retry_strategy = Retry(
            total=3,
            status_forcelist=[413, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

        if not settings.LOCAL_RUN:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION
            )
        else:
            self.s3 = None
        self.src_auth = src_auth
        self.harvest_context = context

    def harvest(self, record):
        calisphere_id = record.get('calisphere-id')
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')

        # Harvest Media File for Record
        media_src = record.get('media_source')
        media_src_file = self._download_src(media_src)
        media_dest = None
        if media_src_file:
            if media_src.get('nuxeo_type') == 'SampleCustomPicture':
                media_dest = derivatives.make_jp2(
                    media_src_file, media_src.get('mimetype'))
            else:
                media_dest = {
                    'media_filepath': media_src_file,
                    'mimetype': media_src.get('mimetype')
                }
        print(f"[{collection_id}, {page_filename}] Media Path: {media_dest}")

        # Harvest Thumbnail File for Record
        thumbnail_src = record.get('thumbnail_source')
        thumbnail_src_file = self._download_src(thumbnail_src)
        thumbnail_dest = None
        if thumbnail_src_file:
            thumbnail_dest = derivatives.make_thumbnail(
                thumbnail_src_file,
                thumbnail_src.get('mimetype')
            )
        print(f"[{collection_id}, {page_filename}] Thumbnail Path: {thumbnail_dest}")

        # Recurse through the record's children (if any)
        child_records = get_child_records(collection_id, calisphere_id)
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

    def _download_src(self, content_src):
        '''
            download source file to local disk
        '''
        if not content_src:
            return None

        filename = content_src.get('filename')
        src_url = content_src.get('url')
        mimetype = content_src.get('mimetype')
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')

        tmp_file_path = os.path.join('/tmp', filename)
        if os.path.exists(tmp_file_path):
            print(
                f"[{collection_id}, {page_filename}] File already exists at "
                f"download destination, using: {tmp_file_path}"
            )
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

        # print(response.headers)
        # print(response.status_code)

        with open(tmp_file_path, 'wb') as f:
            for block in response.iter_content(1024):
                f.write(block)

        print(
            f"[{collection_id}, {page_filename}]: Downloaded file from "
            f"{src_url} to {tmp_file_path} - mimetype: {mimetype} - fsize: "
            f"{os.path.getsize(tmp_file_path)}"
        )
        return tmp_file_path


# {"collection_id": 26098, "mapper_type": "nuxeo", "page_filename": "r-0"}
def harvest_page_content(payload, context):
    if settings.LOCAL_RUN and isinstance(payload, str):
        payload = json.loads(payload)

    collection_id = payload.get('collection_id')
    mapper_type = payload.get('mapper_type')
    page_filename = payload.get('page_filename')
    print(f"[{collection_id}, {page_filename}]: harvest_page_content: {json.dumps(payload)}")

    records = get_mapped_records(collection_id, page_filename)

    auth = None
    if mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    harvester = ContentHarvester(payload, src_auth=auth)

    for record in records:
        print(
            f"[{collection_id}, {page_filename}]: "
            f"harvest record: {record.get('calisphere-id')}"
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
    return len(records)
