import json
import os
import shutil
from collections import Counter

import boto3
import requests
from requests.adapters import HTTPAdapter, Retry


from . import derivatives
from . import settings


class DownloadError(Exception):
    pass


class UnsupportedMimetype(Exception):
    pass


def get_mapped_records(collection_id, page_filename, s3_client) -> list:
    mapped_records = []
    if settings.DATA_SRC["STORE"] == 'file':
        local_path = settings.local_path('mapped_metadata', collection_id)
        page_path = os.path.join(local_path, str(page_filename))
        page = open(page_path, "r")
        mapped_records = json.loads(page.read())
    else:
        page = s3_client.get_object(
            Bucket=settings.DATA_SRC["BUCKET"],
            Key=f"mapped_metadata/{collection_id}/{page_filename}"
        )
        mapped_records = json.loads(page['Body'].read())
    return mapped_records


def write_mapped_record(collection_id, record, s3_client):
    if settings.DATA_DEST["STORE"] == 'file':
        local_path = settings.local_path('mapped_with_content', collection_id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        
        # some ids have slashes
        page_path = os.path.join(
            local_path,
            record.get('calisphere-id').replace(os.sep, '_')
        )
        
        page = open(page_path, "w")
        page.write(json.dumps(record))
    else:
        upload_status = s3_client.put_object(
            Bucket=settings.DATA_DEST["BUCKET"],
            Key=(
                f"mapped_with_content/{collection_id}/"
                f"{record.get('calisphere-id')}"
            ),
            Body=json.dumps(record)
        )
        print(f"Upload status: {upload_status}")


def get_child_records(collection_id, parent_id, s3_client) -> list:
    mapped_child_records = []
    if settings.DATA_SRC["STORE"] == 'file':
        local_path = settings.local_path('mapped_metadata', collection_id)
        children_path = os.path.join(local_path, 'children')

        if os.path.exists(children_path):
            child_pages = [file for file in os.listdir(children_path)
                        if file.startswith(parent_id)]
            for child_page in child_pages:
                child_page_path = os.path.join(children_path, child_page)
                page = open(child_page_path, "r")
                mapped_child_records.extend(json.loads(page.read()))
    else:
        child_pages = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f"mapped_metadata/{collection_id}/children/{parent_id}"
        )
        for child_page in child_pages['Contents']:
            page = s3_client.get_object(
                Bucket=settings.DATA_SRC["BUCKET"],
                Key=child_page['Key']
            )
            mapped_child_records.extend(json.loads(page['Body'].read()))

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
        # TODO: the image harvest md5 thing
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

        if settings.CONTENT_DEST["STORE"] == 's3':
            self.s3 = boto3.client('s3')
        else:
            self.s3 = None


    # returns content = {thumbnail, media, children} where children
    # is an array of the self-same content dictionary
    def harvest(self, record) -> dict:
        calisphere_id = record.get('calisphere-id')
        collection_id = self.harvest_context.get('collection_id')
        page_filename = self.harvest_context.get('page_filename')

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
                self._download(content.src_url, content.tmp_filepath)
            if not content.processed():
                content.create_derivatives()
            content_s3_filepath = self._upload(
                content.dest_prefix, content.derivative_filepath)
            content.set_s3_filepath(content_s3_filepath)

            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}] "
                f"{type(content).__name__} Path: {content.s3_filepath}"
            )
            
            record[field] = {
                'mimetype': content.dest_mime_type,
                'path': content.s3_filepath
            }

        # Recurse through the record's children (if any)
        child_records = get_child_records(
            collection_id, calisphere_id, self.s3)
        if child_records:
            print(
                f"[{collection_id}, {page_filename}, {calisphere_id}]: "
                f"{len(child_records)} children found."
            )
            record['children'] = [self.harvest(c) for c in child_records]

        return record

    def _download(self, url, destination_file):
        '''
            download source file to local disk
        '''
        # Weird how we have to use username/pass to hit this endpoint
        # but we have to use auth token to hit API endpoint
        request = {
            "url": url,
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

        with open(destination_file, 'wb') as f:
            for block in response.iter_content(1024):
                f.write(block)


    def _upload(self, dest_prefix, filepath) -> str:
        '''
            upload file to CONTENT_DEST
        '''
        filename = os.path.basename(filepath)
        dest_path = None

        if settings.CONTENT_DEST["STORE"] == 'file':
            dest_path = os.path.join(
                settings.CONTENT_DEST["PATH"], dest_prefix)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            dest_path = os.path.join(dest_path, filename)
            shutil.copyfile(filepath, dest_path)

        if settings.CONTENT_DEST["STORE"] == 's3':
            dest_path = (
                f"{settings.CONTENT_DEST['PATH']}/{dest_prefix}/{filename}")
            self.s3.upload_file(
                filepath, settings.CONTENT_DEST["BUCKET"], dest_path)

        return dest_path


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

    for i, record in enumerate(records):
        print(
            f"[{collection_id}, {page_filename}]: "
            f"Harvesting record {i}, {record.get('calisphere-id')}"
        )
        # spit out progress so far if an error has been encountered
        try:
            record_with_content = harvester.harvest(record)
            write_mapped_record(
                collection_id, record_with_content, harvester.s3)
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
            raise e

    # reporting aggregate stats
    media_mimetypes = [
        record.get('media', {}).get('mimetype') for record in records]
    thumb_mimetypes = [
        record.get('thumbnail', {}).get('mimetype') for record in records]
    media_source_mimetype = [
        record.get('media_source', {}).get('mimetype') for record in records]
    thumb_source_mimetype = [
        record.get('thumbnail_source', {}).get('mimetype')
        for record in records
    ]
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
