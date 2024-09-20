import hashlib
import os
from typing import Optional

from PIL import Image
from PIL import UnidentifiedImageError
import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import settings
from . import derivatives

from rikolti.utils.storage import upload_file


# an in-memory cache of what we have already downloaded to the worker
# filesystem; this is a src_url: {filepath, md5} dict
downloaded_urls = {}
def conditional_download(download_function):
    """
    Decorator to conditionally download content based on whether we have
    already downloaded it. This is useful for when we are downloading
    the same content multiple times in a single run - for example, when
    the media file is a PDF and the thumbnail file is a jpg derived from
    the same PDF).
    """
    def wrapper(request):
        cache_key = request['url']
        cached_content_metadata = downloaded_urls[cache_key]
        if cached_content_metadata:
            return cached_content_metadata
        else:
            content_metadata = download_function(request)
            if content_metadata:
                downloaded_urls[cache_key] = content_metadata
            return content_metadata
    return wrapper


def conditional_request(cached_responses):
    """
    Decorator to produce a conditional request based on whether we have
    previously downloaded the content and stored etag and last-modified
    response headers.
    """
    def inner_decorator(download_function):
        def wrapper(request):
            cache_key = request['url']
            if cache_key in cached_responses:
                cached_response = cached_responses[cache_key]
            if cached_response:
                request['header'] = cached_response['header']

            response = download_function(request)
            if response.status_code == 304:
                return cached_response['content_metadata']
            else:
                # process response
                content_metadata = {}
                cached_responses[cache_key] = {
                    'header': response.headers,
                    'content_metadata': content_metadata
                }
                return content_metadata
        return wrapper
    return inner_decorator



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


# returns content = {thumbnail, media, children} where children
# is an array of the self-same content dictionary
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

    derivative_filepath = None

    # get media first, sometimes media is used for thumbnail
    media_source = record.get('media_source', {})
    media_source_url = media_source.get('url')
    media_destination_filename = media_source.get(
        'filename', get_url_basename(media_source_url))

    if media_source_url:
        media_metadata = download_media({
            'url': media_source_url,
            'auth': auth
        })
        if media_metadata:
            (media_tmp_filepath, _) = media_metadata
        
        if media_tmp_filepath:
            if media_source.get('nuxeo_type') == 'SampleCustomPicture':
                derivatives.check_media_mimetype(media_source.get('mimetype'))
                derivative_filepath = derivatives.make_jp2(media_tmp_filepath)
                if derivative_filepath:
                    jp2_destination_filename = (
                        f"{media_destination_filename.split('.')[0]}.jp2")
                    record['media'] = {
                        'mimetype': 'image/jp2',
                        'path': upload_content(
                            derivative_filepath, 
                            f"jp2/{collection_id}/{jp2_destination_filename}"),
                        'format': NUXEO_MEDIA_TYPE_MAP.get(media_source.get('nuxeo_type'))
                    }
            else:
                record['media'] = {
                    'mimetype': media_source.get('mimetype'),
                    'path': upload_content(
                        media_tmp_filepath, 
                        f"media/{collection_id}/{media_destination_filename}"
                    ),
                    'format': NUXEO_MEDIA_TYPE_MAP.get(media_source.get('nuxeo_type'))
                }

    # backwards compatibility
    thumbnail_src = record.get('thumbnail_source', record.get('is_shown_by'))
    if isinstance(thumbnail_src, str):
        thumbnail_src = {'url': thumbnail_src}
        record['thumbnail_source'] = thumbnail_src

    thumbnail_src = thumbnail_src or {}
    thumbnail_src_url = thumbnail_src.get('url', '')
    thumbnail_tmp_filepath = (
        f"/tmp/{thumbnail_src.get('filename', get_url_basename(thumbnail_src_url))}")

    if thumbnail_src.get('url'):
        thumbnail_metadata = download_thumbnail({
            'url': thumbnail_src_url, 
            'auth': auth
        })
        if thumbnail_metadata:
            thumbnail_tmp_filepath, downloaded_md5 = thumbnail_metadata

        content_s3_filepath = None
        dimensions = None
        if downloaded_md5 and thumbnail_src.get('mimetype', 'image/jpeg') in ['image/jpeg', 'image/png']:
            try:
                dimensions = get_dimensions(
                    thumbnail_tmp_filepath, record['calisphere-id'])
            except Exception as e:
                print(
                    f"Error getting dimensions for {record['calisphere-id']}: "
                    f"{e}, continuing..."
                )
            else:
                content_s3_filepath = upload_content(
                    thumbnail_tmp_filepath,
                    f"thumbnails/{collection_id}/{downloaded_md5}"
                )
        elif downloaded_md5 and thumbnail_src.get('mimetype', 'image/jpeg') == 'application/pdf':
            derivative_filepath = derivatives.pdf_to_thumb(thumbnail_tmp_filepath)
            if derivative_filepath:
                md5 = hashlib.md5(
                    open(derivative_filepath, 'rb').read()).hexdigest()
                content_s3_filepath = upload_content(
                    derivative_filepath, f"thumbnails/{collection_id}/{md5}"
                )
                dimensions = get_dimensions(derivative_filepath, record['calisphere-id'])
        elif downloaded_md5 and thumbnail_src.get('mimetype', 'image/jpeg') in ['video/mp4','video/quicktime']:
            derivative_filepath = derivatives.video_to_thumb(thumbnail_tmp_filepath)
            if derivative_filepath:
                md5 = hashlib.md5(
                    open(derivative_filepath, 'rb').read()).hexdigest()
                content_s3_filepath = upload_content(
                    derivative_filepath, f"thumbnails/{collection_id}/{md5}"
                )
                dimensions = get_dimensions(derivative_filepath, record['calisphere-id'])

        if content_s3_filepath:
            record['thumbnail'] = {
                'mimetype': 'image/jpeg',
                'path': content_s3_filepath,
                'dimensions': dimensions
            }
    # if media_tmp_filepath and os.path.exists(media_tmp_filepath):
    #     os.remove(media_tmp_filepath)
    #     downloaded_urls.pop(media_source_url, None)
    if thumbnail_src.get('url') and os.path.exists(thumbnail_tmp_filepath):
        os.remove(thumbnail_tmp_filepath)
        downloaded_urls.pop(thumbnail_src.get('url'), None)
    if derivative_filepath and os.path.exists(derivative_filepath):
        os.remove(derivative_filepath)

    return record

def get_dimensions(filepath: str, calisphere_id: str) -> tuple[int, int]:
    try:
        return Image.open(filepath).size
    except UnidentifiedImageError as e:
        raise Exception(
            f"PIL.UnidentifiedImageError for calisphere-id "
            f"{calisphere_id}: {e}"
        )
    except Image.DecompressionBombError as e:
        raise Exception(
            f"PIL.Image.DecompressionBombError for calisphere-id "
            f"{calisphere_id}: {e}"
        )


@conditional_download
def download_media(request: dict) -> Optional[tuple[str, str]]:
    '''
        download source file to local disk
    '''
    url = request['url']
    if request.get('auth') and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")

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
    with open(local_destination, 'wb') as f:
        for block in response.iter_content(1024 * hasher.block_size):
            hasher.update(block)
            f.write(block)
    md5 = hasher.hexdigest()

    return (local_destination, md5)


@conditional_download
def download_thumbnail(request: dict,
                resp_headers_cache: Optional[dict] = None
            ):
    '''
        download source file to local disk
    '''
    url = request['url']
    if request.get('auth') and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")

    if not resp_headers_cache:
        resp_headers_cache = {}
    cached_data = resp_headers_cache.get(url, {})

    request.update({
        "stream": True,
        "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
    })
    if cached_data:
        request['headers'] = {
            'If-None-Match': cached_data.get('If-None-Match'),
            'If-Modified-Since': cached_data.get('If-Modified-Since')
        }
        request['headers'] = {k:v for k,v in request['headers'].items() if v}

    response = http_session.get(**request)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error downloading {url}: {e}")
        return None

    # short-circuit here
    if response.status_code == 304: # 304 - not modified
        return cached_data.get('md5')

    local_destination = f"/tmp/{get_url_basename(request['url'])}"
    hasher = hashlib.new('md5')
    with open(local_destination, 'wb') as f:
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
    resp_headers_cache[url] = cached_data.update(cache_updates)

    return (local_destination, md5)


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