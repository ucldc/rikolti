import hashlib
import os
from datetime import datetime, timedelta
from typing import Optional, Any, Tuple, Callable, Union
from functools import lru_cache
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus

from PIL import Image
from PIL import UnidentifiedImageError
import requests
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from . import settings
from . import derivatives
from .s3_cache import S3Cache

from rikolti.utils.storage import upload_file


s3_cache = os.environ.get('CONTENT_COMPONENT_CACHE')
if s3_cache:
    bucket = urlparse(s3_cache).netloc
    prefix = urlparse(s3_cache).path.lstrip('/')
    persistent_cache = S3Cache(bucket, prefix)
else:
    print("CONTENT_COMPONENT_CACHE not configured, skipping cache check")
    persistent_cache = None

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


def make_thumbnail_src(record, thumbnail_url: str) -> dict:
    """
    provided for backwards compatibility, add a deprecation warning log
    here, to get mappers off using the is_shown_by key
    """
    thumbnail_src = {'url': thumbnail_url}
    record['thumbnail_source'] = thumbnail_src
    return thumbnail_src
    

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

    tmp_files = []
    media_source = record.get('media_source', {})
    media_source_url = media_source.get('url')
    if media_source_url:
        request = ContentRequest(media_source_url, auth)
        media_component, media_tmp_files = create_media_component(
            collection_id, request, media_source)
        tmp_files.extend(media_tmp_files)
        if media_component and media_component.get('path'):
            record['media'] = media_component

    thumbnail_src = record.get(
        'thumbnail_source', 
        record.get('is_shown_by', '')
    )
    if isinstance(thumbnail_src, str):
        thumbnail_src = make_thumbnail_src(record, thumbnail_src)
    thumbnail_src_url = thumbnail_src.get('url')
    if thumbnail_src_url:
        request = ContentRequest(thumbnail_src_url, auth)
        record['thumbnail'], thumbnail_tmp_files = create_thumbnail_component(
            collection_id,
            request,
            thumbnail_src,
            record['calisphere-id']
        )
        tmp_files.extend(thumbnail_tmp_files)

    [os.remove(filepath) for filepath in set(tmp_files)]

    return record


@dataclass(frozen=True)
class ContentRequest(object):
    """
    An immutable, hashable object representing a request for content.
    This is used as the cache key in download_url. Since auth can be
    Any, we need to define our own __hash__ and __eq__ methods for use
    with the lru cache.
    """
    url: str
    auth: Any

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        return self.url == other.url

    def __post_init__(self):
        if self.auth and urlparse(self.url).scheme != 'https':
            raise Exception(
                f"Basic auth not over https is a bad idea! {self.url}")


@lru_cache(maxsize=10)
def download_url(request: ContentRequest):
    """
    Download a file from a given URL and return:
    {
        'path': str,            # local filesystem path to the downloaded
                                  content file
        'md5': str,             # md5 hash of the content file
        'size': int             # size of the downloaded file in bytes
        'Content-Type': str,    # Content-Type header from the response
        'ETag': str,            # ETag header from the response
        'Last-Modified': str,   # Last-Modified header from the response
    }

    This is cached in an in-memory lru_cache to avoid downloading the
    same url multiple times - as is the case when the media file is a
    video or pdf file and the thumbnail file is a jpg derived from the
    same video or pdf file. The cache is limited to 10 entries, but
    since we first download the media source file and then immediately
    download the thumbnail source file, I think we could limit this to 2
    items.
    """
    get_request = asdict(request)
    get_request.update({
        "stream": True,
        "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
    })
    response = http_session.get(**get_request)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error downloading {request.url}: {e}")
        return None

    local_destination = f"/tmp/{get_url_basename(request.url)}"
    hasher = hashlib.new('md5')
    source_size = 0
    with open(local_destination, 'wb') as f:
        for block in response.iter_content(1024 * hasher.block_size):
            hasher.update(block)
            f.write(block)
            source_size += len(block)
    md5 = hasher.hexdigest()

    return {
        'path': local_destination,
        'md5': md5,
        'size': source_size,
        'Content-Type': response.headers.get('Content-Type'),
        'ETag': response.headers.get('ETag'),
        'Last-Modified': response.headers.get('Last-Modified'),
    }


def in_last_seven_days(iso_date: str) -> bool:
    """
    Return True if the date is within the last 7 days
    """
    date = datetime.fromisoformat(iso_date)
    today = datetime.today()
    seven_days_ago = today - timedelta(days=7)
    return seven_days_ago <= date <= today


CreateComponentFunc = Callable[...,Tuple[Optional[dict], list]]


def content_component_cache(component_type: str) -> Callable[
    [CreateComponentFunc], CreateComponentFunc]:
    """
    decorator to cache content components in a persistent cache
    don't love how this is implemented, but it's a start
    for one, check_component_cache arguments seem pretty tightly
    coupled to create_media_component and create_thumbnail_component
    """
    def inner(create_component: CreateComponentFunc) -> CreateComponentFunc:
        def check_component_cache(
                collection_id: Union[str, int],
                request: ContentRequest,
                *args: Any, **kwargs: Any
            ) -> Tuple[Optional[dict], list]:

            if not persistent_cache:
                # always a cache miss, don't cache this
                component, tmp_files = create_component(
                    collection_id, request, *args, **kwargs)
                if component:
                    return {**component, 'from-cache': False}, tmp_files
                else:
                    return None, tmp_files

            # Do a head request to get the current ETag and
            # Last-Modified values, used to create a cache key
            if 'nuxeo' in request.url:
                # The S3 presigned URLs from Nuxeo are good for GET requests only
                # so do a GET request that mimics a head request
                head_resp = http_session.get(
                    **asdict(request),
                    headers={"Range": "bytes=0-0"}
                )
            else:
                head_resp = http_session.head(**asdict(request), allow_redirects=True)
            if not (
                head_resp.headers.get('ETag') or
                head_resp.headers.get('Last-Modified')
            ):
                print(
                    f"{component_type}: No ETag or Last-Modified headers, "
                    "checking cache and judging cache hit based on URL and "
                    "date since content component creation"
                )
                # Create cache key without ETag or Last-Modified
                cache_key = '/'.join([
                    str(collection_id),
                    quote_plus(request.url),
                    component_type
                ])
            else:
                # Create specific cache key
                cache_key = '/'.join([
                    str(collection_id),
                    quote_plus(request.url),
                    component_type,
                    quote_plus(head_resp.headers.get('ETag', '')),
                    quote_plus(head_resp.headers.get('Last-Modified', ''))
                ])

            # Check cache for component or create component
            component = persistent_cache.get(cache_key)
            if component:
                date_component_created = (component
                    ['component_content_harvest_metadata']
                    ['date_content_component_created']
                )
                print(
                    f"Retrieved {component_type} component created on "
                    f"{date_component_created} from cache for "
                    f"{request.url}"
                )
                # if the cache key is specific with etag and/or
                # last-modified, so we have a true cache hit
                if len(cache_key.split('/')) == 5:
                    return {**component, 'from-cache': True}, []

                # if the cache key is not specific, check if the cache
                # hit was created within the last week
                elif in_last_seven_days(date_component_created):
                    return {**component, 'from-cache': True}, []

                print(f"Cache hit at key {cache_key} is older than 7 days, "
                      "re-creating component")

            component, tmp_files = create_component(
                collection_id, request, *args, **kwargs)
            print(f"Created {component_type} component for {request.url}")
            # set cache key to the component
            if component:
                if component['path'] is not None:
                    persistent_cache[cache_key] = component
                return {**component, 'from-cache': False}, tmp_files
            else:
                return None, tmp_files

        return check_component_cache
    return inner


def get_dimensions(
        filepath: str, 
        calisphere_id: str = 'not provided') -> Optional[tuple[int, int]]:
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


@content_component_cache('media')
def create_media_component(
        collection_id: Union[str, int], 
        request: ContentRequest, 
        mapped_media_source: dict[str, str]
    ) -> Tuple[Optional[dict], list]:
    '''
        download source file to local disk
    '''
    media_dest_filename = mapped_media_source.get(
        'filename', get_url_basename(request.url))
    if not media_dest_filename:
        print(f"Could not determine filename for {request.url}")
        return None, []

    source_component = download_url(request)
    if not source_component:
        return None, []

    media_tmp_filepath = source_component['path']
    tmp_filepaths = [media_tmp_filepath]
    mapped_mimetype = mapped_media_source.get('mimetype')
    mapped_nuxeotype = mapped_media_source.get('nuxeo_type')
    mimetype = None
    content_s3_filepath = None

    if mapped_nuxeotype == 'SampleCustomPicture':
        derivatives.check_media_mimetype(mapped_mimetype or '')
        derivative_filepath = derivatives.make_jp2(media_tmp_filepath)
        if derivative_filepath:
            jp2_destination_filename = (
                f"{media_dest_filename.split('.')[0]}.jp2")
            content_s3_filepath = upload_content(
                derivative_filepath, 
                f"jp2/{collection_id}/{jp2_destination_filename}"
            )
            tmp_filepaths.append(derivative_filepath)
            mimetype = 'image/jp2'
    else:
        content_s3_filepath = upload_content(
            media_tmp_filepath, 
            f"media/{collection_id}/{media_dest_filename}"
        )
        mimetype = mapped_mimetype

    media_component = {
        'mimetype': mimetype,
        'path': content_s3_filepath,
        'format': NUXEO_MEDIA_TYPE_MAP.get(mapped_nuxeotype or ''),
        'component_content_harvest_metadata': {
            'md5': source_component['md5'],
            'src_content-type': source_component['Content-Type'],
            'mapped_mimetype': mapped_mimetype,
            'src_size': source_component['size'],
            'date_content_component_created': datetime.now().isoformat()
        }
    }

    return media_component, tmp_filepaths


@content_component_cache('thumbnail')
def create_thumbnail_component(
        collection_id: Union[str, int], 
        request: ContentRequest, 
        mapped_thumbnail_source: dict[str, str],
        record_context: str
    ) -> Tuple[Optional[dict], list]:
    '''
        download source file to local disk
    '''
    source_component = download_url(request)
    if not source_component:
        return None, []

    thumbnail_tmp_filepath = source_component['path']
    tmp_filepaths = [thumbnail_tmp_filepath]
    thumbnail_md5 = source_component['md5']

    content_s3_filepath = None
    dimensions = None
    mapped_mimetype = mapped_thumbnail_source.get('mimetype', 'image/jpeg')
    if mapped_mimetype in ['image/jpeg', 'image/png']:
        dimensions = get_dimensions(thumbnail_tmp_filepath, record_context)
        content_s3_filepath = upload_content(
            thumbnail_tmp_filepath,
            f"thumbnails/{collection_id}/{thumbnail_md5}"
        )
    elif mapped_mimetype == 'application/pdf':
        derivative_filepath = derivatives.pdf_to_thumb(thumbnail_tmp_filepath)
        if derivative_filepath:
            content_s3_filepath = upload_content(
                derivative_filepath, f"thumbnails/{collection_id}/{thumbnail_md5}"
            )
            dimensions = get_dimensions(derivative_filepath, record_context)
            tmp_filepaths.append(derivative_filepath)
    elif mapped_mimetype in ['video/mp4','video/quicktime']:
        derivative_filepath = derivatives.video_to_thumb(thumbnail_tmp_filepath)
        if derivative_filepath:
            content_s3_filepath = upload_content(
                derivative_filepath, f"thumbnails/{collection_id}/{thumbnail_md5}"
            )
            dimensions = get_dimensions(derivative_filepath, record_context)
            tmp_filepaths.append(derivative_filepath)

    thumbnail_component = {
        'mimetype': 'image/jpeg',
        'path': content_s3_filepath,
        'dimensions': dimensions,
        'component_content_harvest_metadata': {
            'md5': source_component['md5'],
            'src_content-type': source_component['Content-Type'],
            'mapped_mimetype': mapped_thumbnail_source.get('mimetype'),
            'src_size': source_component['size'],
            'date_content_component_created': datetime.now().isoformat()
        }
    }

    return thumbnail_component, tmp_filepaths


def upload_content(filepath: str, destination: str) -> str:
    '''
        upload file to RIKOLTI_CONTENT
    '''
    content_root = os.environ.get("RIKOLTI_CONTENT", 'file:///tmp')
    content_path = f"{content_root.rstrip('/')}/{destination}"
    upload_file(filepath, content_path)
    return content_path