import hashlib
import os
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import settings
from .content_types import Media, Thumbnail

from rikolti.utils.storage import upload_file
from rikolti.utils.versions import (
    get_mapped_page_content, get_child_directories, get_child_pages,
    get_version
)


def get_child_records(mapped_page_path, parent_id) -> list:
    mapped_child_records = []
    children = get_child_pages(mapped_page_path)
    children = [page for page in children
                if (page.rsplit('/')[-1]).startswith(parent_id)]
    for child in children:
        mapped_child_records.extend(get_mapped_page_content(child))
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


# returns content = {thumbnail, media, children} where children
# is an array of the self-same content dictionary
def harvest_record_content(
            record: dict,
            collection_id,
            mapped_page_path,
            rikolti_mapper_type: Optional[str] = None,
            download_cache: Optional[dict] = None,
            ) -> dict:

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    src_auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        src_auth = (settings.NUXEO_USER, settings.NUXEO_PASS)

    calisphere_id = record.get('calisphere-id')
    media = record.get('media_source')
    thumbnail = record.get('thumbnail_source', record.get('is_shown_by'))
    if isinstance(thumbnail, str):
        record['thumbnail_source'] = {'url': thumbnail}
        thumbnail = {'url': thumbnail}

    # get media first, sometimes media is used for thumbnail
    if media:
        media_content = Media(media)
        if not media_content.downloaded():
            md5 = download_content(
                media_content.src_url, 
                media_content.tmp_filepath, 
                src_auth, 
                download_cache
            )
        elif download_cache:
            md5 = download_cache.get(
                media_content.src_url,
                hashlib.md5(open(media_content.tmp_filepath, 'rb').read()).hexdigest()
            )
        if not media_content.processed():
            media_content.create_derivatives()

        if type(media_content).__name__ == 'Thumbnail':
            dest_filename = md5
        else:
            dest_filename = os.path.basename(media_content.derivative_filepath)

        dest_path = f"{media_content.dest_prefix}/{collection_id}/{dest_filename}"
        content_s3_filepath = upload_content(media_content.derivative_filepath, dest_path)
        
        media_content.set_s3_filepath(content_s3_filepath)
        # print(
        #     f"{collection_id:<6}: page: {page_filename}, record: {calisphere_id} "
        #     f"{type(media_content).__name__} Path: {content.s3_filepath}"
        # )

        record['media'] = {
            'mimetype': media_content.dest_mime_type,
            'path': media_content.s3_filepath
        }

    if thumbnail:
        thumbnail_content = Thumbnail(thumbnail)
        if not thumbnail_content.downloaded():
            md5 = download_content(
                thumbnail_content.src_url, 
                thumbnail_content.tmp_filepath, 
                src_auth, 
                download_cache
            )
        elif download_cache:
            md5 = download_cache.get(
                thumbnail_content.src_url,
                hashlib.md5(open(thumbnail_content.tmp_filepath, 'rb').read()).hexdigest()
            )
        if not thumbnail_content.processed():
            thumbnail_content.create_derivatives()

        if type(thumbnail_content).__name__ == 'Thumbnail':
            dest_filename = md5
        else:
            dest_filename = os.path.basename(thumbnail_content.derivative_filepath)

        dest_path = f"{thumbnail_content.dest_prefix}/{collection_id}/{dest_filename}"
        content_s3_filepath = upload_content(thumbnail_content.derivative_filepath, dest_path)
        
        thumbnail_content.set_s3_filepath(content_s3_filepath)
        # print(
        #     f"{collection_id:<6}: page: {page_filename}, record: {calisphere_id} "
        #     f"{type(thumbnail_content).__name__} Path: {content.s3_filepath}"
        # )

        record['thumbnail'] = {
            'mimetype': thumbnail_content.dest_mime_type,
            'path': thumbnail_content.s3_filepath
        }

    # Recurse through the record's children (if any)
    mapped_version = get_version(collection_id, mapped_page_path)
    child_directories = get_child_directories(mapped_version)
    if child_directories:
        print(f"CHILD DIRECTORIES: {child_directories}")
    if child_directories:
        child_records = get_child_records(
            mapped_page_path, calisphere_id)
        if child_records:
            print(
                f"{mapped_page_path}: {len(child_records)} children found of "
                f"record {calisphere_id}."
            )
            record['children'] = [
                harvest_record_content(
                    child_record, 
                    collection_id, 
                    mapped_page_path, 
                    rikolti_mapper_type,
                    download_cache=download_cache
                )
                for child_record in child_records
            ]

    return record


def download_content(url: str, 
                destination_file: str, 
                src_auth: Optional[tuple[str, str]] = None, 
                cache: Optional[dict] = None
            ):
    '''
        download source file to local disk
    '''
    http = configure_http_session()
    if src_auth and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")

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