import hashlib
import os
from datetime import datetime
from typing import Optional

from PIL import Image
from PIL import UnidentifiedImageError
import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import settings
from . import derivatives

from rikolti.utils.storage import upload_file

content_component_cache = dict()
in_memory_cache = dict()


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
http_session = configure_http_session()


# type Organization should actually be type CustomFile.
# Adding workaround for now.
NUXEO_MEDIA_TYPE_MAP = {
    "SampleCustomPicture": "image",
    "CustomAudio": "audio",
    "CustomVideo": "video",
    "CustomFile": "file",
    "Organization": "file",
    "CustomThreeD": "3d"
}


def get_url_basename(url: str) -> Optional[str]:
    """
    For a given url, return the basename of the path, handling any
    query parameters or fragments.
    """
    url_path = urlparse(url).path
    url_path_parts = [p for p in url_path.split('/') if p]
    return url_path_parts[-1] if url_path_parts else None


def get_thumb_src(record: dict) -> dict:
    """
    provided for backwards compatibility, add a deprecation warning log
    here, to get mappers off using the is_shown_by key
    """
    thumb_src = {'url': record.get('is_shown_by', '')}
    record['thumbnail_source'] = thumb_src
    return thumb_src
    

# returns a mapped metadata record with content urls at the
# 'media' and 'thumbnail' keys
def harvest_record_content(
            record: dict,
            collection_id,
            rikolti_mapper_type: Optional[str] = None,
            ) -> dict:

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)

    media_source = record.get('media_source', {})
    media_source_url = media_source.get('url')
    if media_source_url:
        request = {'url': media_source_url, 'auth': auth}
        record['media'] = create_media_component(collection_id, request, media_source)

    thumbnail_src = record.get('thumbnail_source', get_thumb_src(record))
    thumbnail_src_url = thumbnail_src.get('url')
    if thumbnail_src_url:
        request = {'url': thumbnail_src_url, 'auth': auth}
        record['thumbnail'] = create_thumbnail_component(collection_id, request, thumbnail_src, record['calisphere-id'])

    return record


def get_dimensions(filepath: str, calisphere_id: str = 'not provided') -> Optional[tuple[int, int]]:
    try:
        return Image.open(filepath).size
    except UnidentifiedImageError as e:
        print(
            f"PIL.UnidentifiedImageError for calisphere-id "
            f"{calisphere_id}: {e}\n"
            f"Error getting dimensions for {calisphere_id}: "
            f"{e}, continuing..."
        )
        return None
    except Image.DecompressionBombError as e:
        print(
            f"PIL.Image.DecompressionBombError for calisphere-id "
            f"{calisphere_id or 'not provided'}: {e}\n"
            f"Error getting dimensions for {calisphere_id}: "
            f"{e}, continuing..."
        )
        return None


def create_media_component(collection_id, request: dict, media_source: dict[str, str]) -> Optional[dict[str, str]]:
    '''
        download source file to local disk
    '''
    url = request['url']
    if request.get('auth') and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")
    head_resp = http_session.head(**request)

    media_component = content_component_cache.get('|'.join([
        collection_id,
        request['url'],
        'media',
        head_resp.headers.get('ETag', ''),
        head_resp.headers.get('Last-Modified', '')]))
    if media_component:
        media_component['from-cache'] = True
        print(f"Retrieved media component from cache for {request['url']}")
        return media_component

    media_dest_filename = media_source.get('filename', get_url_basename(url))

    media_source_component = in_memory_cache.get(request['url'])
    if not media_source_component:
        request.update({
            "stream": True,
            "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
        })
        response = http_session.get(**request)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error downloading {url}: {e}")
            return None

        local_destination = f"/tmp/{get_url_basename(request['url'])}"
        hasher = hashlib.new('md5')
        source_size = 0
        with open(local_destination, 'wb') as f:
            for block in response.iter_content(1024 * hasher.block_size):
                hasher.update(block)
                f.write(block)
                source_size += len(block)
        md5 = hasher.hexdigest()

        media_source_component = {
            'path': local_destination,
            'md5': md5,
            'Content-Type': response.headers.get('Content-Type'),
            'ETag': response.headers.get('ETag'),
            'Last-Modified': response.headers.get('Last-Modified'),
            'size': source_size
        }
        in_memory_cache[request['url']] = media_source_component
    
    media_tmp_filepath = media_source_component['path']
    media_component = dict()
    if media_source.get('nuxeo_type') == 'SampleCustomPicture':
        derivatives.check_media_mimetype(media_source.get('mimetype', ''))
        derivative_filepath = derivatives.make_jp2(media_tmp_filepath)
        if derivative_filepath:
            jp2_destination_filename = (
                f"{media_dest_filename.split('.')[0]}.jp2")
            media_component = {
                'mimetype': 'image/jp2',
                'path': upload_content(
                    derivative_filepath, 
                    f"jp2/{collection_id}/{jp2_destination_filename}"),
                'format': NUXEO_MEDIA_TYPE_MAP.get(media_source.get('nuxeo_type', ''))
            }
    else:
        media_component = {
            'mimetype': media_source.get('mimetype'),
            'path': upload_content(
                media_tmp_filepath, 
                f"media/{collection_id}/{media_dest_filename}"
            ),
            'format': NUXEO_MEDIA_TYPE_MAP.get(media_source.get('nuxeo_type', ''))
        }

    media_component.update({
        'md5': media_source_component['md5'],
        'src_content-type': media_source_component['Content-Type'],
        'src_size': media_source_component['size'],
        'date_content_component_created': datetime.now().isoformat()
    })
    content_component_cache['|'.join([
        collection_id, 
        request['url'], 
        'media', 
        head_resp.headers.get('Etag', ''), 
        head_resp.headers.get('Last-Modified', '')
    ])] = media_component
    print(f"Created media component for {request['url']}")
    media_component['from-cache'] = False
    return media_component


def create_thumbnail_component(collection_id, request: dict, thumb_src: dict[str, str], record_context) -> Optional[dict[str, str]]:
    '''
        download source file to local disk
    '''
    url = request['url']
    if request.get('auth') and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")

    head_resp = http_session.head(**request)
    thumb_component = content_component_cache.get('|'.join([
        collection_id,
        request['url'],
        'thumbnail',
        head_resp.headers.get('ETag', ''),
        head_resp.headers.get('Last-Modified', '')]))
    if thumb_component:
        thumb_component['from-cache'] = True
        print(f"Retrieved thumbnail component from cache for {request['url']}")
        return thumb_component

    thumb_source_component = in_memory_cache.get(request['url'])
    if not thumb_source_component:
        request.update({
            "stream": True,
            "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
        })
        response = http_session.get(**request)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error downloading {url}: {e}")
            return None

        local_destination = f"/tmp/{get_url_basename(request['url'])}"
        hasher = hashlib.new('md5')
        source_size = 0
        with open(local_destination, 'wb') as f:
            for block in response.iter_content(1024 * hasher.block_size):
                hasher.update(block)
                f.write(block)
                source_size += len(block)
        md5 = hasher.hexdigest()

        thumb_source_component = {
            'path': local_destination,
            'md5': md5,
            'Content-Type': response.headers.get('Content-Type'),
            'ETag': response.headers.get('ETag'),
            'Last-Modified': response.headers.get('Last-Modified'),
            'size': source_size
        }
        in_memory_cache[request['url']] = thumb_source_component


    thumbnail_tmp_filepath = thumb_source_component['path']
    thumbnail_md5 = thumb_source_component['md5']
    thumb_src_mimetype = thumb_src.get('mimetype', 'image/jpeg')

    content_s3_filepath = None
    dimensions = None
    downloaded_md5 = thumbnail_md5.get('md5')
    if thumb_src_mimetype in ['image/jpeg', 'image/png']:
        dimensions = get_dimensions(thumbnail_tmp_filepath, record_context)
        content_s3_filepath = upload_content(
            thumbnail_tmp_filepath,
            f"thumbnails/{collection_id}/{downloaded_md5}"
        )
    elif thumb_src_mimetype == 'application/pdf':
        derivative_filepath = derivatives.pdf_to_thumb(thumbnail_tmp_filepath)
        if derivative_filepath:
            content_s3_filepath = upload_content(
                derivative_filepath, f"thumbnails/{collection_id}/{downloaded_md5}"
            )
            dimensions = get_dimensions(derivative_filepath, record_context)
    elif thumb_src_mimetype in ['video/mp4','video/quicktime']:
        derivative_filepath = derivatives.video_to_thumb(thumbnail_tmp_filepath)
        if derivative_filepath:
            content_s3_filepath = upload_content(
                derivative_filepath, f"thumbnails/{collection_id}/{downloaded_md5}"
            )
            dimensions = get_dimensions(derivative_filepath, record_context)

    thumb_component = {
        'mimetype': 'image/jpeg',
        'path': content_s3_filepath,
        'dimensions': dimensions,
        'md5': thumb_source_component['md5'],
        'src_content-type': thumb_source_component['Content-Type'],
        'src_size': thumb_source_component['size'],
        'date_content_component_created': datetime.now().isoformat()
    }
    content_component_cache['|'.join([
            collection_id, 
            request['url'], 
            'thumbnail', 
            head_resp.headers.get('Etag', ''), 
            head_resp.headers.get('Last-Modified', '')
        ])] = thumb_component
    print(f"Created thumbnail component for {request['url']}")
    thumb_component['from-cache'] = False
    return thumb_component


def upload_content(filepath: str, destination: str, md5_cache: Optional[dict] = None) -> str:
    '''
        upload file to CONTENT_ROOT
    '''
    if not md5_cache:
        md5_cache = {}

    filename = os.path.basename(destination)
    if md5_cache.get(filename, {}).get('path'):
        return md5_cache[filename]['path']

    content_root = os.environ.get("CONTENT_ROOT", 'file:///tmp')
    content_path = f"{content_root.rstrip('/')}/{destination}"
    upload_file(filepath, content_path)

    # (mime, dimensions) = image_info(filepath)
    md5_cache[filename] = {
        # 'mime': mime,
        # 'dimensions': dimensions,
        'path': content_path
    }
    return content_path