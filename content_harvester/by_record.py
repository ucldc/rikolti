import hashlib
import os
from typing import Optional

from PIL import Image
from PIL import UnidentifiedImageError
import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import settings
from .content_types import Media, Thumbnail, check_media_mimetype
from . import derivatives

from rikolti.utils.storage import upload_file


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


# returns content = {thumbnail, media, children} where children
# is an array of the self-same content dictionary
def harvest_record_content(
            record: dict,
            collection_id,
            rikolti_mapper_type: Optional[str] = None,
            ) -> dict:

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    request = {}
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        request = {'auth': (settings.NUXEO_USER, settings.NUXEO_PASS)}

    http = configure_http_session()

    downloaded_urls = {}        # downloaded is a src_url: filepath dict
    derivative_filepath = None

    # get media first, sometimes media is used for thumbnail
    media = Media(record.get('media_source', {}))
    if media:
        request.update({'url': media.src_url})

        downloaded_md5 = download_content(request, http, media.tmp_filepath)
        if downloaded_md5:
            downloaded_urls[media.src_url] = (media.tmp_filepath, downloaded_md5)

        if media.src_nuxeo_type == 'SampleCustomPicture' and downloaded_md5:
            check_media_mimetype(media.src_mime_type)
            derivative_filepath = derivatives.make_jp2(media.tmp_filepath)
            if derivative_filepath:
                basename = os.path.basename(derivative_filepath)
                record['media'] = {
                    'mimetype': 'image/jp2',
                    'path': upload_content(
                        derivative_filepath, f"jp2/{collection_id}/{basename}"),
                    'format': NUXEO_MEDIA_TYPE_MAP.get(media.src_nuxeo_type)
                }
        elif downloaded_md5:
            record['media'] = {
                'mimetype': media.src_mime_type,
                'path': upload_content(
                    media.tmp_filepath, 
                    f"media/{collection_id}/{media.src_filename}"
                ),
                'format': NUXEO_MEDIA_TYPE_MAP.get(media.src_nuxeo_type)
            }

    # backwards compatibility
    thumbnail_src = record.get('thumbnail_source', record.get('is_shown_by'))
    if isinstance(thumbnail_src, str):
        thumbnail_src = {'url': thumbnail_src}
        record['thumbnail_source'] = thumbnail_src

    thumbnail = Thumbnail(thumbnail_src or {})
    if thumbnail:
        if downloaded_urls.get(thumbnail.src_url):
            thumbnail.tmp_filepath, downloaded_md5 = (
                downloaded_urls[thumbnail.src_url])
        else:
            request.update({'url': thumbnail.src_url})
            downloaded_md5 = download_content(
                request, http, thumbnail.tmp_filepath)

        content_s3_filepath = None
        dimensions = None
        if downloaded_md5 and thumbnail.src_mime_type in ['image/jpeg', 'image/png']:
            try:
                dimensions = get_dimensions(
                    thumbnail.tmp_filepath, record['calisphere-id'])
            except Exception as e:
                print(
                    f"Error getting dimensions for {record['calisphere-id']}: "
                    f"{e}, continuing..."
                )
            else:
                content_s3_filepath = upload_content(
                    thumbnail.tmp_filepath,
                    f"thumbnails/{collection_id}/{downloaded_md5}"
                )
        elif downloaded_md5 and thumbnail.src_mime_type == 'application/pdf':
            derivative_filepath = derivatives.pdf_to_thumb(thumbnail.tmp_filepath)
            if derivative_filepath:
                md5 = hashlib.md5(
                    open(derivative_filepath, 'rb').read()).hexdigest()
                content_s3_filepath = upload_content(
                    derivative_filepath, f"thumbnails/{collection_id}/{md5}"
                )
                dimensions = get_dimensions(derivative_filepath, record['calisphere-id'])
        elif downloaded_md5 and thumbnail.src_mime_type in ['video/mp4','video/quicktime']:
            derivative_filepath = derivatives.video_to_thumb(thumbnail.tmp_filepath)
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
    if media and os.path.exists(media.tmp_filepath):
        os.remove(media.tmp_filepath)
        downloaded_urls.pop(media.src_url, None)
    if thumbnail and os.path.exists(thumbnail.tmp_filepath):
        os.remove(thumbnail.tmp_filepath)
        downloaded_urls.pop(thumbnail.src_url, None)
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

def download_content(request: dict, http,
                destination_file: str, 
                cache: Optional[dict] = None
            ):
    '''
        download source file to local disk
    '''
    url = request['url']
    if request.get('auth') and urlparse(url).scheme != 'https':
        raise Exception(f"Basic auth not over https is a bad idea! {url}")

    if not cache:
        cache = {}
    cached_data = cache.get(url, {})

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

    response = http.get(**request)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error downloading {url}: {e}")
        return None

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
    cache[url] = cached_data.update(cache_updates)

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