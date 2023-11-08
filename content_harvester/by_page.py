import hashlib
import json
import os
import shutil
from collections import Counter
from typing import Optional

import boto3
import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import derivatives
from . import settings

from rikolti.utils.rikolti_storage import RikoltiStorage

class DownloadError(Exception):
    pass


class UnsupportedMimetype(Exception):
    pass


def get_mapped_records(collection_id, page_filename) -> list:
    mapped_records = []
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_SRC_URL}/{collection_id}/mapped_metadata/{page_filename}")
    mapped_records = json.loads(rikolti_data.get_page_content())
    return mapped_records


def write_mapped_record(collection_id, record):
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_DEST_URL}/{collection_id}/mapped_with_content/"
        f"{record.get('calisphere-id').replace(os.sep, '_')}"
    )
    rikolti_data.put_page_content(json.dumps(record))


def write_mapped_page(collection_id, page, records):
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_DEST_URL}/{collection_id}/mapped_with_content/{page}"
    )
    rikolti_data.put_page_content(json.dumps(records))


def get_child_records(collection_id, parent_id, s3_client) -> list:
    mapped_child_records = []
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_SRC_URL}/{collection_id}/mapped_metadata/children")
    children = rikolti_data.list_pages(recursive=False, relative=False)
    if rikolti_data.data_store == 'file':
        children = [page for page in children
                    if os.path.basename(page).startswith(parent_id)]
    for child in children:
        child_data = RikoltiStorage(child)
        mapped_child_records.extend(json.loads(child_data.get_page_content()))
    return mapped_child_records


class Content(object):
    def __init__(self, content_src):
        self.missing = True if not content_src else False
        self.src_url = content_src.get('url')
        self.src_filename = content_src.get(
            'filename',
            list(
                filter(
                    lambda x: bool(x), content_src.get('url', '').split('/')
                )
            )[-1]
        )
        self.src_mime_type = content_src.get('mimetype')
        self.tmp_filepath = os.path.join('/tmp', self.src_filename)
        self.derivative_filepath = None
        self.s3_filepath = None

    def downloaded(self):
        return os.path.exists(self.tmp_filepath)

    def processed(self):
        return (
            self.derivative_filepath and 
            os.path.exists(self.derivative_filepath)
        )

    def set_s3_filepath(self, s3_filepath):
        self.s3_filepath = s3_filepath

    def __bool__(self):
        return not self.missing

    def __del__(self):
        if self.downloaded() and self.tmp_filepath != self.s3_filepath:
            os.remove(self.tmp_filepath)
        if self.processed() and self.derivative_filepath != self.s3_filepath:
            os.remove(self.derivative_filepath)


class Media(Content):
    def __init__(self, content_src):
        super().__init__(content_src)
        self.src_nuxeo_type = content_src.get('nuxeo_type')
        if self.src_nuxeo_type == 'SampleCustomPicture':
            self.dest_mime_type = 'image/jp2'
            self.dest_prefix = "jp2"
        else:
            self.dest_mime_type = self.src_mime_type
            self.dest_prefix = "media"

    def create_derivatives(self):
        self.derivative_filepath = self.tmp_filepath
        if self.src_nuxeo_type == 'SampleCustomPicture':
            try:
                self.check_mimetype(self.src_mime_type)
                self.derivative_filepath = derivatives.make_jp2(
                    self.tmp_filepath)
            except UnsupportedMimetype as e:
                print(
                    "ERROR: nuxeo type is SampleCustomPicture, "
                    "but mimetype is not supported"
                )
                raise(e)

    def check_mimetype(self, mimetype):
        ''' do a basic pre-check on the object to see if we think it's
        something know how to deal with '''
        valid_types = [
            'image/jpeg', 'image/gif', 'image/tiff', 'image/png',
            'image/jp2', 'image/jpx', 'image/jpm'
        ]

        # see if we recognize this mime type
        if mimetype in valid_types:
            print(
                f"Mime-type '{mimetype}' was pre-checked and recognized as "
                "something we can try to convert."
            )
        elif mimetype in ['application/pdf']:
            raise UnsupportedMimetype(
                f"Mime-type '{mimetype}' was pre-checked and recognized as "
                "something we don't want to convert."
            )
        else:
            raise UnsupportedMimetype(
                f"Mime-type '{mimetype}' was unrecognized. We don't know how "
                "to deal with this"
            )


class Thumbnail(Content):
    def __init__(self, content_src):
        super().__init__(content_src)
        self.src_mime_type = content_src.get('mimetype', 'image/jpeg')
        self.dest_mime_type = 'image/jpeg' # do we need this? 
        self.dest_prefix = "thumbnails"

    def create_derivatives(self):
        self.derivative_filepath = None
        if self.src_mime_type == 'image/jpeg':
            self.derivative_filepath = self.tmp_filepath
        elif self.src_mime_type == 'application/pdf':
            self.derivative_filepath = derivatives.pdf_to_thumb(
                self.tmp_filepath)
        elif self.src_mime_type == 'video/mp4':
            self.derivative_filepath = derivatives.video_to_thumb(
                self.tmp_filepath)
        else:
            raise UnsupportedMimetype(f"thumbnail: {self.src_mime_type}")

    def check_mimetype(self, mimetype):
        if mimetype not in ['image/jpeg', 'application/pdf', 'video/mp4']:
            raise UnsupportedMimetype(f"thumbnail: {mimetype}")


class ContentHarvester(object):

    # context = {'collection_id': '12345', 'page_filename': '1.jsonl'}
    def __init__(self, collection_id, page_filename, src_auth=None):
        self.http = requests.Session()

        retry_strategy = Retry(
            total=3,
            status_forcelist=[413, 429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

        self.src_auth = src_auth
        self.collection_id = collection_id
        self.page_filename = page_filename

        if settings.CONTENT_DEST["STORE"] == 's3':
            self.s3 = boto3.client('s3')
        else:
            self.s3 = None

    # returns content = {thumbnail, media, children} where children
    # is an array of the self-same content dictionary
    def harvest(self, record: dict, download_cache: Optional[dict] = None) -> dict:
        calisphere_id = record.get('calisphere-id')

        # maintain backwards compatibility to 'is_shown_by' field
        thumbnail_src = record.get(
            'thumbnail_source', record.get('is_shown_by'))
        if isinstance(thumbnail_src, str):
            record['thumbnail_source'] = {'url': thumbnail_src}

        # get media first, sometimes media is used for thumbnail
        content_types = [(Media, 'media'), (Thumbnail, 'thumbnail')]
        for content_cls, field in content_types:
            content = record.get(f"{field}_source")
            if not content:
                continue

            content = content_cls(content)
            if not content.downloaded():
                md5 = self._download(content.src_url, content.tmp_filepath, download_cache)
            elif download_cache:
                md5 = download_cache.get(
                    content.src_url,
                    hashlib.md5(open(content.tmp_filepath, 'rb').read()).hexdigest()
                )
            if not content.processed():
                content.create_derivatives()

            if field == 'thumbnail':
                dest_filename = md5
            else:
                dest_filename = os.path.basename(content.derivative_filepath)

            content_s3_filepath = self._upload(
                content.dest_prefix, dest_filename, content.derivative_filepath)
            content.set_s3_filepath(content_s3_filepath)

            # print(
            #     f"[{self.collection_id}, {self.page_filename}, {calisphere_id}] "
            #     f"{type(content).__name__} Path: {content.s3_filepath}"
            # )
            
            record[field] = {
                'mimetype': content.dest_mime_type,
                'path': content.s3_filepath
            }

        # Recurse through the record's children (if any)
        child_records = get_child_records(
            self.collection_id, calisphere_id, self.s3)
        if child_records:
            print(
                f"[{self.collection_id}, {self.page_filename}, {calisphere_id}]: "
                f"{len(child_records)} children found."
            )
            record['children'] = [self.harvest(c, download_cache=download_cache) for c in child_records]

        return record

    def _download(self, url: str, destination_file: str, cache: Optional[dict] = None):
        '''
            download source file to local disk
        '''
        if self.src_auth and urlparse(url).scheme != 'https':
            raise DownloadError(f"Basic auth not over https is a bad idea! {url}")

        if not cache:
            cache = {}

        cached_data = cache.get(url, {})

        request = {
            "url": url,
            "auth": self.src_auth,
            "stream": True,
            "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
        }
        if cached_data:
            request['headers'] = {
                'If-None-Match': cached_data.get('If-None-Match'),
                'If-Modified-Since': cached_data.get('If-Modified-Since')
            }
            request['headers'] = {k:v for k,v in request['headers'].items() if v}

        response = self.http.get(**request)
        response.raise_for_status()

        # short-circuit here
        if response.status_code == 304: # 304 - not modified
            return cached_data.get('md5')

        hasher = hashlib.new('md5')
        with open(destination_file, 'wb') as f:
            for block in response.iter_content(1024 * hasher.block_size):
                hasher.update(block)
                f.write(block)
        md5 = hasher.hexdigest()

        cache_updates = {
            'If-None-Match': response.headers.get('ETag'),
            'If-Modified-Since': response.headers.get('Last-Modified'),
            'Mime-Type': response.headers.get('Content-type'),
            'md5': md5
        }
        cache_updates = {k:v for k,v in cache_updates.items() if v}
        cache['url'] = cached_data.update(cache_updates)

        return md5

    def _upload(self, dest_prefix, dest_filename, filepath, cache: Optional[dict] = None) -> str:
        '''
            upload file to CONTENT_DEST
        '''
        if not cache:
            cache = {}

        if cache.get(dest_filename, {}).get('path'):
            return cache[dest_filename]['path']

        dest_path = ''

        if settings.CONTENT_DEST["STORE"] == 'file':
            dest_path = os.path.join(
                settings.CONTENT_DEST["PATH"], dest_prefix)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            dest_path = os.path.join(dest_path, dest_filename)
            shutil.copyfile(filepath, dest_path)

        if settings.CONTENT_DEST["STORE"] == 's3':
            dest_path = (
                f"{settings.CONTENT_DEST['PATH']}/{dest_prefix}/{dest_filename}")
            self.s3.upload_file(
                filepath, settings.CONTENT_DEST["BUCKET"], dest_path)

        # (mime, dimensions) = image_info(filepath)
        cache_updates = {
            # 'mime': mime,
            # 'dimensions': dimensions,
            'path': dest_path
        }
        cache[dest_filename] = cache_updates

        return dest_path


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo", "page_filename": "r-0"}
def harvest_page_content(collection_id, page_filename, **kwargs):
    rikolti_mapper_type = kwargs.get('rikolti_mapper_type')

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    harvester = ContentHarvester(
        collection_id=collection_id,
        page_filename=page_filename,
        src_auth=auth
    )

    records = get_mapped_records(collection_id, page_filename)
    print(
        f"[{collection_id}, {page_filename}]: "
        f"Harvesting content for {len(records)} records"
    )

    for i, record in enumerate(records):
        # print(
        #     f"[{collection_id}, {page_filename}]: "
        #     f"Harvesting record {i}, {record.get('calisphere-id')}"
        # )
        # spit out progress so far if an error has been encountered
        try:
            record_with_content = harvester.harvest(record)
            # write_mapped_record(
            #     collection_id, record_with_content)
            if not record_with_content.get('thumbnail'):
                warn_level = "ERROR"
                if 'sound' in record.get('type', []):
                    warn_level = "WARNING"
                print(
                    f"[{collection_id}, {page_filename}]: "
                    f"{record.get('calisphere-id')}: {warn_level} - "
                    f"NO THUMBNAIL: {record.get('type')}"
                )

        except Exception as e:
            print(
                f"[{collection_id}, {page_filename}, "
                f"{record.get('calisphere-id')}]: ERROR: {e}"
            )
            print(f"Exiting after harvesting {i} of {len(records)} items "
                  f"in page {page_filename} of collection {collection_id}")

    write_mapped_page(collection_id, page_filename, records)

    media_source = [r for r in records if r.get('media_source')]
    media_harvested = [r for r in records if r.get('media')]
    media_src_mimetypes = [r.get('media_source', {}).get('mimetype') for r in records]
    media_mimetypes = [r.get('media', {}).get('mimetype') for r in records]

    if media_source:
        print(
            f"[{collection_id}, {page_filename}]: Harvested "
            f"{len(media_harvested)} media records - "
            f"{len(media_source)}/{len(records)} described a media source"
        )
        print(
            f"[{collection_id}, {page_filename}]: Source Media Mimetypes: "
            f"{Counter(media_src_mimetypes)}"
        )
        print(
            f"[{collection_id}, {page_filename}]: Destination Media "
            f"Mimetypes: {Counter(media_mimetypes)}"
        )

    thumb_source = [
        r for r in records if r.get('thumbnail_source', r.get('is_shown_by'))]
    thumb_harvested = [r for r in records if r.get('thumbnail')]
    thumb_src_mimetypes = [
        r.get('thumbnail_source', {}).get('mimetype') for r in records]
    thumb_mimetypes = [r.get('thumbnail', {}).get('mimetype') for r in records]
    print(
        f"[{collection_id}, {page_filename}]: Harvested "
        f"{len(thumb_harvested)} media records - "
        f"{len(thumb_source)}/{len(records)} described a thumbnail source"
    )
    print(
        f"[{collection_id}, {page_filename}]: Source Thumbnail Mimetypes: "
        f"{Counter(thumb_src_mimetypes)}"
    )
    print(
        f"[{collection_id}, {page_filename}]: Destination Thumbnail "
        f"Mimetypes: {Counter(thumb_mimetypes)}"
    )

    child_contents = [len(record.get('children', [])) for record in records]

    return {
        'thumb_source': Counter(thumb_src_mimetypes),
        'thumb_mimetypes': Counter(thumb_mimetypes),
        'media_source': Counter(media_src_mimetypes),
        'media_mimetypes': Counter(media_mimetypes),
        'children': sum(child_contents),
        'records': len(records)
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content using a page of mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('page_filename', help="Page Filename")
    parser.add_argument('--nuxeo', action="store_true", help="Use Nuxeo auth")
    args = parser.parse_args()
    arguments = {
        'collection_id': args.collection_id,
        'page_filename': args.page_filename,
    }
    if args.nuxeo:
        arguments['rikolti_mapper_type'] = 'nuxeo.nuxeo'
    print(harvest_page_content(**arguments))
