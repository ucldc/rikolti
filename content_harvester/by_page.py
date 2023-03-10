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


def get_mapped_records(collection_id, page_filename, s3_client) -> list:
    mapped_records = []
    if settings.METADATA_SRC == 'local':
        local_path = settings.local_path(
            'mapped_metadata', collection_id)
        page_path = os.sep.join([local_path, str(page_filename)])
        page = open(page_path, "r")
        mapped_records = json.loads(page.read())
    else:
        page = s3_client.get_object(
            Bucket=settings.S3_BUCKET,
            Key=f"mapped_metadata/{collection_id}/{page_filename}"
        )
        mapped_records = json.loads(page['Body'].read())
    return mapped_records


def write_mapped_records(collection_id, page_filename, harvested_page, s3_client):
    if settings.METADATA_DEST == 'local':
        local_path = settings.local_path(
            'mapped_with_content', collection_id)
        page_path = os.sep.join([local_path, str(page_filename)])
        page = open(page_path, "w")
        page.write(json.dumps(harvested_page))
    else:
        upload_status = s3_client.put_object(
            Bucket=settings.S3_BUCKET,
            Key=f"mapped_with_content/{collection_id}/{page_filename}",
            Body=json.dumps(harvested_page)
        )
        print(f"Upload status: {upload_status}")


def get_child_records(collection_id, parent_id, s3_client) -> list:
    mapped_child_records = []
    if settings.METADATA_SRC == 'local':
        local_path = settings.local_path('mapped_metadata', collection_id)
        children_path = os.sep.join([local_path, 'children'])

        if os.path.exists(children_path):
            child_pages = [file for file in os.listdir(children_path)
                        if file.startswith(parent_id)]
            for child_page in child_pages:
                child_page_path = os.sep.join([children_path, child_page])
                page = open(child_page_path, "r")
                mapped_child_records.extend(json.loads(page.read()))
    else:
        child_pages = s3_client.list_objects_v2(
            Bucket=settings.S3_BUCKET,
            Prefix=f"mapped_metadata/{collection_id}/children/{parent_id}"
        )
        for child_page in child_pages['Contents']:
            page = s3_client.get_object(
                Bucket=settings.S3_BUCKET,
                Key=child_page['Key']
            )
            mapped_child_records.extend(json.loads(page['Body'].read()))

    return mapped_child_records


class UnsupportedMimetype(Exception):
    pass


def check_mimetype(mimetype):
    ''' do a basic pre-check on the object to see if we think it's
    something know how to deal with '''
    valid_types = ['image/jpeg', 'image/gif', 'image/tiff', 'image/png', 'image/jp2', 'image/jpx', 'image/jpm']

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
            f"Mime-type '{mimetype}' was unrecognized. We don't know how to "
            "deal with this"
        )


def check_thumb_mimetype(mimetype):
    if mimetype not in ['image/jpeg', 'application/pdf', 'video/mp4']:
        raise UnsupportedMimetype(f"thumbnail: {mimetype}")


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

        if settings.CONTENT_DEST == 's3':
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                aws_session_token=settings.AWS_SESSION_TOKEN,
                region_name=settings.AWS_REGION
            )
        else:
            self.s3 = None

    # returns media_dest = {media_filepath, mimetype}
    def harvest_media(self, media_src, media_src_file) -> dict:
        if not media_src_file:
            return None

        media_filepath = None
        if media_src.get('nuxeo_type') == 'SampleCustomPicture':
            try:
                derivatives.check_mimetype(media_src.get('mimetype'))
                media_filepath = derivatives.make_jp2(media_src_file)
            except UnsupportedMimetype as e:
                print(e)
                media_filepath = media_src_file

            media_dest = {'mimetype': 'image/jp2'}
            if settings.CONTENT_DEST == 'local':
                media_dest['media_filepath'] = media_filepath
            else:
                media_dest['media_filepath'] = self.upload_to_s3(
                    'jp2', media_filepath)
        else:
            media_filepath = media_src_file
            media_dest = {'mimetype': media_src.get('mimetype')}
            if settings.CONTENT_DEST == 'local':
                media_dest['media_filepath'] = media_filepath
            else:
                media_dest['media_filepath'] = self.upload_to_s3(
                    'media', media_filepath)

        return media_dest

    # returns thumbnail_dest = {thumbnail_filepath, mimetype}
    def harvest_thumbnail(self, thumbnail_src, thumbnail_src_file) -> dict:
        # set default mimetype to 'image/jpeg' in cases where no mimetype is 
        # specified (aka, all non-nuxeo cases)
        if not thumbnail_src_file:
            return None

        thumb_mimetype = thumbnail_src.get('mimetype', 'image/jpeg')

        thumbnail_filepath = None
        if thumb_mimetype == 'image/jpeg':
            thumbnail_filepath = thumbnail_src_file
        elif thumb_mimetype == 'application/pdf':
            thumbnail_filepath = derivatives.pdf_to_thumb(
                thumbnail_src_file)
        elif thumb_mimetype == 'video/mp4':
            thumbnail_filepath = derivatives.video_to_thumb(
                thumbnail_src_file)
        else:
            raise UnsupportedMimetype(f"thumbnail: {thumb_mimetype}")

        # TODO: the image harvest md5 thing + do we need thumbnail destination
        # mimetype if it's always image/jpeg? (is it always image/jpeg?)
        thumbnail_dest = {'mimetype': 'image/jpeg'}

        if settings.CONTENT_DEST == 'local':
            thumbnail_dest['thumbnail_filepath'] = thumbnail_filepath
        else:
            thumbnail_dest['thumbnail_filepath'] = self.upload_to_s3(
                'thumbnails', thumbnail_filepath)
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
        if media_src_file:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"Media Source: {media_src}, Temp File: {media_src_file}, "
                f"fsize: {os.path.getsize(media_src_file)}"
            )
        media_dest = self.harvest_media(
            calisphere_id, media_src, media_src_file)
        print(
            f"[{collection_id}, {page_filename}, {calisphere_id}] "
            f"Media Path: {media_dest}"
        )

        # Harvest Thumbnail File for Record
        thumbnail_src = record.get('thumbnail_source')

        # this makes it so we don't have to re-write isShownBy mappings for
        # non-nuxeo sources
        if isinstance(thumbnail_src) == str:
            thumbnail_src = {'url': thumbnail_src}

        thumbnail_src_file = self._download_src(thumbnail_src)
        if thumbnail_src_file:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"Thumb Source: {thumbnail_src}, Temp File: "
                f"{thumbnail_src_file}, fsize: "
                f"{os.path.getsize(thumbnail_src_file)}"
            )
        thumbnail_dest = self.harvest_thumbnail(
            thumbnail_src, thumbnail_src_file)
        print(
            f"[{collection_id}, {page_filename}, {calisphere_id}] "
            f"Thumbnail Path: {thumbnail_dest}"
        )

        if media_src_file:
            os.remove(media_src_file)
        if thumbnail_src_file:
            os.remove(thumbnail_src_file)

        # Recurse through the record's children (if any)
        child_records = get_child_records(collection_id, calisphere_id, self.s3)

        print(
            f"[{collection_id}, {page_filename}, {calisphere_id}]: "
            f"{len(child_records)} children found."
        )

        children_media = []
        for child in child_records:
            children_media.append(self.harvest(child))

        record['thumbnail'] = thumbnail_dest
        if media_dest:
            record['media'] = media_dest
        if children_media:
            record['children'] = children_media
        return record

    def _download_src(self, content_src) -> str:
        '''
            download source file to local disk
        '''
        if not content_src:
            return None

        src_url = content_src.get('url')
        # filename defaults to last part of the url (filter clause handles urls
        # with a trailing slash where the [-1] last item is an empty string)
        filename = content_src.get(
            'filename',
            list(filter(lambda x: bool(x), src_url.split('/')))[-1]
        )

        tmp_file_path = os.path.join('/tmp', filename)
        if os.path.exists(tmp_file_path):
            return tmp_file_path

        # Weird how we have to use username/pass to hit this endpoint
        # but we have to use auth token to hit API endpoint
        request = {
            "url": src_url,
            "stream": True,
            "timeout": (12.05, (60 * 10) + 0.05)  # connect, read
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

    def upload_to_s3(self, s3_prefix, filepath) -> str:
        '''
            upload file to s3
        '''
        s3_key = f"{s3_prefix}/{os.path.basename(filepath)}"
        s3_url = f"{settings.S3_BASE_URL}/{s3_key}"
        self.s3.upload_file(filepath, settings.S3_CONTENT_BUCKET, s3_key)
        return s3_url

# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo", "page_filename": "r-0"}
def harvest_page_content(collection_id, page_filename, **kwargs):
    rikolti_mapper_type = kwargs.get('rikolti_mapper_type')

    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    harvester = ContentHarvester(context={
        'collection_id': collection_id, 
        'page_filename': page_filename
    }, src_auth=auth)

    records = get_mapped_records(collection_id, page_filename, harvester.s3)
    print(
        f"[{collection_id}, {page_filename}]: "
        f"Harvesting content for {len(records)} records"
    )

    for record in records:
        print(
            f"[{collection_id}, {page_filename}]: "
            f"Harvesting record {record.get('calisphere-id')}"
        )

        record_with_content = harvester.harvest(record)

        if not record_with_content.get('thumbnail'):
            warn_level = "ERROR"
            if 'sound' in record.get('type', []):
                warn_level = "WARNING"
            print(
                f"[{collection_id}, {page_filename}]: "
                f"{record.get('calisphere-id')}: {warn_level} - "
                f"NO THUMBNAIL: {record.get('type')}"
            )

    write_mapped_records(collection_id, page_filename, records, harvester.s3)

    # reporting aggregate stats
    media_mimetypes = [record.get('media', {}).get('mimetype') for record in records]
    thumb_mimetypes = [record.get('thumbnail', {}).get('mimetype') for record in records]
    media_source_mimetype = [record.get('media_source', {}).get('mimetype') for record in records]
    thumb_source_mimetype = [record.get('thumbnail_source', {}).get('mimetype') for record in records]
    child_contents = [len(record.get('children', [])) for record in records]
    
    return {
        'thumb_source': Counter(thumb_source_mimetype),
        'thumb_mimetypes': Counter(thumb_mimetypes),
        'media_source': Counter(media_source_mimetype),
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
    print(harvest_page_content(arguments))
