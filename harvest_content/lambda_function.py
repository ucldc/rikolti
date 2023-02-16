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


def download_source(http, content_source, auth=None):
    '''
        download source file to local disk
    '''
    if not content_source:
        return None

    filename = content_source.get('filename')
    src_url = content_source.get('url')
    mimetype = content_source.get('mimetype')

    tmp_file_path = os.path.join('/tmp', filename)
    print('downloading...')
    if os.path.exists(tmp_file_path):
        print(f"File already exists, using: {tmp_file_path}")
        return tmp_file_path

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    request = {
        "url": src_url,
        "stream": True,
        "timeout": (12.05, (60 * 10) + 0.05) # connect, read
    }
    if auth:
        request['auth'] = auth

    try:
        response = http.get(**request)
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
        f"Downloaded file from {src_url} to "
        f"{tmp_file_path} - mimetype: {mimetype} "
        f"- fsize: {os.path.getsize(tmp_file_path)}"
    )
    return tmp_file_path


def create_requests_session():
    retry_strategy = Retry(
        total=3,
        status_forcelist=[413, 429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


def create_s3_session():
    return boto3.client(
        's3',
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION
    )


def get_mapped_records(collection_id, page_filename) -> dict:
    if settings.DATA_SRC == 'local':
        print(f"Getting local mapped records for {collection_id}, {page_filename}")
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
        print("NO CHILDREN")
    else:
        print("YO CHILDREN")
    return mapped_child_records


def harvest_media(http, calisphere_id, media_source):
    media_source_filepath = None
    media_filepath = None
    # thumbnail_s3_path = None

    if media_source:
        try:
            media_source_filepath = download_source(
                http,
                media_source,
                auth=(settings.NUXEO_USER, settings.NUXEO_PASS)
            )
        except DownloadError as err:
            raise DownloadError(f"[{calisphere_id}]: {err}")

    if media_source_filepath:
        if media_source.get('nuxeo_type') == 'SampleCustomPicture':
            media_filepath = derivatives.make_jp2(media_source_filepath)
        else:
            print(f"not a jp2 sitch: {media_source_filepath}")
            media_filepath = media_source_filepath

    # if media_filepath:
    #     media_s3_path = save_thumbnail(s3, media_filepath)

    return media_filepath


def harvest_thumbnail(http, calisphere_id, thumbnail_source):
    thumbnail_source_filepath = None
    thumbnail_filepath = None
    # thumbnail_s3_path = None

    if thumbnail_source:
        try:
            thumbnail_source_filepath = download_source(
                http,
                thumbnail_source,
                auth=(settings.NUXEO_USER, settings.NUXEO_PASS)
            )
        except DownloadError as err:
            raise DownloadError(f"[{calisphere_id}]: {err}")

    if thumbnail_source_filepath:
        try:
            thumbnail_filepath = derivatives.make_thumbnail(
                thumbnail_source_filepath,
                thumbnail_source.get('mimetype')
            )
        except Exception as err:
            raise Exception(f"[{calisphere_id}]: {err}")

    # if thumbnail_filepath:
    #     thumbnail_s3_path = save_thumbnail(s3, thumbnail_filepath)

    return thumbnail_filepath


def harvest_record_content(mapper_type, http, record, collection_id):
    print(f"harvest record: {record.get('calisphere-id')}")
    if mapper_type == 'nuxeo.nuxeo':
        media = harvest_media(
            http,
            record.get('calisphere-id'),
            record.get('media_source')
        )
        print(f"Media Path: {media}")
        child_records = get_child_records(
            collection_id,
            record.get('calisphere-id')
        )
        for child in child_records:
            harvest_record_content(mapper_type, http, child, collection_id)

    thumbnail = harvest_thumbnail(
        http,
        record.get('calisphere-id'),
        record.get('thumbnail_source')
    )
    if thumbnail:
        # update record
        print(f"Thumbnail Path: {thumbnail}")
    else:
        warn_level = "ERROR"
        if 'sound' in record.get('type', []):
            warn_level = "WARNING"
        print(
            f"{record.get('calisphere-id')}: {warn_level} - NO THUMBNAIL: "
            f"{record.get('type')}"
        )


# {"collection_id": 26098, "mapper_type": "nuxeo", "page_filename": "r-0"}
def harvest_page_content(payload, context):
    if settings.LOCAL_RUN and isinstance(payload, str):
        payload = json.loads(payload)
    print(f"harvest_page_content: {json.dumps(payload)}")

    records = get_mapped_records(
        payload.get('collection_id'),
        payload.get('page_filename')
    )
    mapper_type = payload.get('mapper_type')

    http = create_requests_session()
    # s3 = create_s3_session()
    for record in records:
        harvest_record_content(
            mapper_type, http, record, payload.get('collection_id'))

    return records

