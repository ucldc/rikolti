import hashlib
import json
import os
from collections import Counter
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from .content_types import Media, Thumbnail
from . import settings

from rikolti.utils.storage import upload_file
from rikolti.utils.versions import (
    get_mapped_page, get_child_directories, get_child_pages, get_child_page,
    get_version, put_content_data_page
)

class DownloadError(Exception):
    pass


def get_child_records(mapped_page_path, parent_id) -> list:
    mapped_child_records = []
    children = get_child_pages(mapped_page_path)
    children = [page for page in children
                if (page.rsplit('/')[-1]).startswith(parent_id)]
    for child in children:
        mapped_child_records.extend(get_child_page(child))
    return mapped_child_records


def configure_http_session() -> requests.Session:
    http = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[413, 429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


def harvest_content(content, collection_id, http, src_auth, download_cache):
    if not content.downloaded():
        md5 = download_content(
            content.src_url, 
            content.tmp_filepath, 
            http, 
            src_auth, 
            download_cache
        )
    elif download_cache:
        md5 = download_cache.get(
            content.src_url,
            hashlib.md5(open(content.tmp_filepath, 'rb').read()).hexdigest()
        )
    if not content.processed():
        content.create_derivatives()

    if type(content).__name__ == 'Thumbnail':
        dest_filename = md5
    else:
        dest_filename = os.path.basename(content.derivative_filepath)

    dest_path = f"{content.dest_prefix}/{collection_id}/{dest_filename}"
    content_s3_filepath = upload_content(content.derivative_filepath, dest_path)
    
    content.set_s3_filepath(content_s3_filepath)
    # print(
    #     f"[{collection_id}, {page_filename}, {calisphere_id}] "
    #     f"{type(content).__name__} Path: {content.s3_filepath}"
    # )

    return {
        'mimetype': content.dest_mime_type,
        'path': content.s3_filepath
    }


# returns content = {thumbnail, media, children} where children
# is an array of the self-same content dictionary
def harvest_record(record: dict,
            collection_id,
            page_filename,
            mapped_page_path,
            http: requests.Session,
            src_auth: Optional[tuple[str,str]] = None,
            download_cache: Optional[dict] = None,
            ) -> dict:
    calisphere_id = record.get('calisphere-id')

    # maintain backwards compatibility to 'is_shown_by' field
    thumbnail_src = record.get(
        'thumbnail_source', record.get('is_shown_by'))
    if isinstance(thumbnail_src, str):
        record['thumbnail_source'] = {'url': thumbnail_src}

    # get media first, sometimes media is used for thumbnail
    media = record.get('media_source')
    if media:
        record['media'] = harvest_content(
            Media(media), collection_id, http, src_auth, download_cache)
    thumbnail = record.get('thumbnail_source')
    if thumbnail:
        record['thumbnail'] = harvest_content(
            Thumbnail(thumbnail), collection_id, http, src_auth, download_cache)

    # Recurse through the record's children (if any)
    mapped_version = get_version(
        collection_id, mapped_page_path)
    child_directories = get_child_directories(mapped_version)
    print(f"CHILD DIRECTORIES: {child_directories}")
    if child_directories:
        child_records = get_child_records(
            mapped_page_path, calisphere_id)
        if child_records:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"{len(child_records)} children found."
            )
            record['children'] = [
                harvest_record(
                    child_record, 
                    collection_id, 
                    page_filename, 
                    mapped_page_path, 
                    http, 
                    src_auth, 
                    download_cache=download_cache
                )
                for child_record in child_records
            ]

    return record


def download_content(url: str, 
                destination_file: str, 
                http: requests.Session, 
                src_auth: Optional[tuple[str, str]] = None, 
                cache: Optional[dict] = None
            ):
    '''
        download source file to local disk
    '''
    if src_auth and urlparse(url).scheme != 'https':
        raise DownloadError(f"Basic auth not over https is a bad idea! {url}")

    if not cache:
        cache = {}
    cached_data = cache.get(url, {})

    request = {
        "url": url,
        "auth": src_auth,
        "stream": True,
        "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
    }
    if cached_data:
        request['headers'] = {
            'If-None-Match': cached_data.get('If-None-Match'),
            'If-Modified-Since': cached_data.get('If-Modified-Since')
        }
        request['headers'] = {k:v for k,v in request['headers'].items() if v}

    response = http.get(**request)
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


def upload_content(filepath: str, destination: str, cache: Optional[dict] = None) -> str:
    '''
        upload file to CONTENT_ROOT
    '''
    if not cache:
        cache = {}

    filename = os.path.basename(destination)
    if cache.get(filename, {}).get('path'):
        return cache[filename]['path']

    content_root = os.environ.get("CONTENT_ROOT", 'file:///tmp')
    content_path = f"{content_root.rstrip('/')}/{destination}"
    upload_file(filepath, content_path)

    # (mime, dimensions) = image_info(filepath)
    cache[filename] = {
        # 'mime': mime,
        # 'dimensions': dimensions,
        'path': content_path
    }
    return content_path


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo", "page_filename": "file:///rikolti_data/r-0"}
def harvest_page_content(collection_id, mapped_page_path, content_data_version, **kwargs):
    rikolti_mapper_type = kwargs.get('rikolti_mapper_type')
    page_filename = os.path.basename(mapped_page_path)

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    http_session = configure_http_session()

    records = json.loads(get_mapped_page(mapped_page_path))
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
            record_with_content = harvest_record(
                record, 
                collection_id, 
                page_filename, 
                mapped_page_path, 
                http_session, 
                auth
            )
            # put_content_data_page(
            #     json.dumps(record_with_content), 
            #     record_with_content.get('calisphere-id').replace(os.sep, '_') + ".json",
            #     content_data_version
            # )
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
            raise(e)

    put_content_data_page(
        json.dumps(records), page_filename, content_data_version)

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
        'thumb_source_mimetypes': Counter(thumb_src_mimetypes),
        'thumb_mimetypes': Counter(thumb_mimetypes),
        'media_source_mimetypes': Counter(media_src_mimetypes),
        'media_mimetypes': Counter(media_mimetypes),
        'children': sum(child_contents),
        'records': len(records)
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content using a page of mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('mapped_page_path', help="URI-formatted path to a mapped metadata page")
    parser.add_argument('content_data_version', help="URI-formatted path to a content data version")
    parser.add_argument('--nuxeo', action="store_true", help="Use Nuxeo auth")
    args = parser.parse_args()
    arguments = {
        'collection_id': args.collection_id,
        'mapped_page_path': args.mapped_page_path,
        'content_data_version': args.content_data_version
    }
    if args.nuxeo:
        arguments['rikolti_mapper_type'] = 'nuxeo.nuxeo'
    print(harvest_page_content(**arguments))
