import sys
import os
from file_fetchers.fetcher import Fetcher
from md5s3stash import md5s3stash
import json
import tempfile
import subprocess

S3_PUBLIC_BUCKET = os.environ['S3_PUBLIC_BUCKET']
S3_PRIVATE_BUCKET = os.environ['S3_PRIVATE_BUCKET']
S3_MEDIA_INSTRUCTIONS_FOLDER = os.environ['S3_MEDIA_INSTRUCTIONS_FOLDER']
S3_CONTENT_FILES_FOLDER = os.environ['S3_CONTENT_FILES_FOLDER']
BASIC_USER = os.environ['BASIC_USER']
BASIC_PASS = os.environ['BASIC_PASS']
MAGICK_CONVERT = os.environ.get('MAGICK_CONVERT', '/usr/bin/convert')

class NuxeoFetcher(Fetcher):
    def __init__(self, collection_id, **kwargs):
        super(NuxeoFetcher, self).__init__(collection_id, **kwargs)

    def fetch_files(self):
        """ Fetch, create and stash all files needed by Calisphere for a Nuxeo collection.
            This is based on the media_instructions.jsonl for each object.

            The media_instructions dict is updated as the files are fetched and this becomes
            the media.json file for the object.
        """
        for s3key in self.build_instruction_list():
            instructions = self.fetch_instructions(s3key)

            # stash parent files
            if instructions.get('contentFile'):
                instructions['contentFile'] = self.stash_content_file(instructions)
                instructions['thumbnail'] = self.stash_thumbnail(instructions)

            # stash child files
            if instructions.get('children'):
                for child in instructions['children']:
                    if child.get('contentFile'):
                        child['contentFile'] = self.stash_content_file(child)
                        child['thumbnail'] = self.stash_thumbnail(child)

            # set object thumbnail md5hash
            instructions['objectThumbnail'] = self.set_object_thumbnail(instructions)

            # stash media.json
            self.stash_media_json(s3key, instructions)

    def stash_content_file(self, component):
        if component['contentFile']['mime-type'].startswith('image/'):
            component['contentFile'] = self.stash_jp2(component)
        else:
            fetch_request = self.build_fetch_request(component)
            identifier = component['calisphere-id']
            filename = component['contentFile']['filename']
            component['contentFile']['s3_key'] = Fetcher.stash_content_file(self, identifier, filename, fetch_request)

        return component['contentFile']

    def stash_thumbnail(self, component):
        '''
            The url for the nuxeo thumbnail will be in the media instructions for images and videos
            We need to create and stash a thumbnail image for PDFs
            Other file types (e.g. audio) do not have a thumbnail image
        '''
        if component['thumbnail']:
            (md5hash, mime_type, dimensions) = Fetcher.stash_thumbnail(self, component['thumbnail']['url'])
            component['thumbnail']['md5hash'] = md5hash
            component['thumbnail']['mime-type'] = mime_type
            component['thumbnail']['dimensions'] = dimensions
        elif component['contentFile']['mime-type'] == 'application/pdf':
            component['thumbnail'] = self.stash_pdf_thumbnail(component)

        return component['thumbnail']

    def build_fetch_request(self, component):
        """
        timeouts based on those used by nuxeo-python-client
        see: https://github.com/nuxeo/nuxeo-python-client/blob/master/nuxeo/constants.py
        but tweaked to be slightly larger than a multiple of 3, which is recommended
        in the requests documentation.
        see: https://docs.python-requests.org/en/master/user/advanced/#timeouts
        """
        timeout_connect = 12.05
        timeout_read = (60 * 10) + 0.05
        source_url = component['contentFile']['url']
        request = {
            'url': source_url,
            'auth': (BASIC_USER, BASIC_PASS),
            'stream': True,
            'timeout': (timeout_connect, timeout_read)
        }
        return request

    def stash_jp2(self, component):
        """ create a jp2 copy of the image and stash it on s3 """
        basename_no_ext = self.get_basename_no_ext(component['contentFile']['filename'])
        filename_jp2 = f"{basename_no_ext}.jp2"
        s3_key = f"{S3_CONTENT_FILES_FOLDER}/{self.collection_id}/{component['calisphere-id']}::{filename_jp2}"

        if self.clean_stash or not self.already_stashed(S3_PUBLIC_BUCKET, s3_key):
            source_fullpath = self.fetch_to_temp(component)
            jp2_fullpath = f"{tempfile.gettempdir()}/{filename_jp2}"

            # create jp2 copy of first page/layer/frame of source image
            rate = 10 # factor of compression. 20 means 20 times compressed.
            args = [MAGICK_CONVERT, "-quiet", "-format", "-jp2", "-define", f"jp2:rate={rate}", f"{source_fullpath}[0]", jp2_fullpath]
            subprocess.run(args, check=True)

            # stash on s3
            self.s3.upload_file(jp2_fullpath, S3_PUBLIC_BUCKET, s3_key)
            print(f"stashed on s3: s3://{S3_PUBLIC_BUCKET}/{s3_key}")

            os.remove(source_fullpath)
            os.remove(jp2_fullpath)

        component['contentFile'].update({
            "filename": filename_jp2,
            "mime-type": "image/jp2",
            "s3_key": s3_key
        })

        return component['contentFile']

    def stash_pdf_thumbnail(self, component):
        """ create a jpeg image of the first page of the PDF and stash on s3 """
        # TODO if the thumbnail already exists on s3, we can skip this. Use hash_cache?
        basename_no_ext = self.get_basename_no_ext(component['contentFile']['filename'])
        filename_png = f"first_page-{basename_no_ext}.png"

        # fetch pdf to /tmp
        pdf_fullpath = self.fetch_to_temp(component)
        png_fullpath = f"{tempfile.gettempdir()}/{filename_png}"

        # create png of first page of PDF
        args = [MAGICK_CONVERT, "-quiet", "-strip", "-format", "png", "-quality", "75", f"{pdf_fullpath}[0]", png_fullpath]
        subprocess.run(args, check=True)

        # stash on s3
        stasher = md5s3stash(localpath=png_fullpath)
        stasher.stash()

        os.remove(pdf_fullpath)
        os.remove(png_fullpath)

        component['thumbnail'] = {
            "filename": filename_png,
            "mime-type": stasher.mime_type,
            "md5hash": stasher.md5hash,
            "dimensions": stasher.dimensions
        }

        return component['thumbnail']

    def get_basename_no_ext(self, filename):
        return f"{os.path.splitext(os.path.basename(filename))[0]}"

    def fetch_to_temp(self, component):
        # download file
        fetch_request = self.build_fetch_request(component)
        response = self.http.get(**fetch_request)
        response.raise_for_status()

        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        with tmpfile as f:
            for block in response.iter_content(chunk_size=None):
                f.write(block)

        return tmpfile.name

    def build_instruction_list(self):
        prefix = f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}/"

        response = self.s3.list_objects_v2(
            Bucket=S3_PRIVATE_BUCKET,
            Prefix=prefix
        )

        try:
            return [content['Key'] for content in response['Contents'] if content['Key'] != f"{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}/"]
        except KeyError:
            print(f"No media instruction files found at s3://{S3_MEDIA_INSTRUCTIONS_FOLDER}/{self.collection_id}/")
            return []

    def fetch_instructions(self, s3key):
        response = self.s3.get_object(
            Bucket=S3_PRIVATE_BUCKET,
            Key=s3key
        )
        instructions = json.loads(response['Body'].read())

        return instructions

    def set_object_thumbnail(self, instructions):
        ''' set md5hash of thumbnail for object.

            for complex objects, this may be different from the thumbnail of
            the parent component.
        '''
        # try to find a thumbnail for an image type component first
        # check parent
        md5hash = self.get_thumb_md5_image_source_only(instructions)
        if md5hash:
            return md5hash

        # check children
        if instructions.get('children'):
            for child in instructions['children']:
                md5hash = self.get_thumb_md5_image_source_only(child)
                if md5hash:
                    return md5hash

        # failing that, find any thumbnail
        # check parent
        md5hash = self.get_thumb_md5(instructions)
        if md5hash:
            return md5hash

        # check children
        if instructions.get('children'):
            for child in instructions['children']:
                md5hash = self.get_thumb_md5(child)
                return md5hash

        return None

    def get_thumb_md5_image_source_only(self, component):
        ''' get md5hash for component of type image '''
        md5hash = None
        try:
            if component['contentFile']['mime-type'].startswith('image/'):
                md5hash = component['thumbnail']['md5hash']
        except KeyError:
            pass

        return md5hash

    def get_thumb_md5(self, component):
        ''' get md5hash for any component '''
        md5hash = None
        try:
            md5hash = component['thumbnail']['md5hash']
        except KeyError:
            pass

        return md5hash

    def stash_media_json(self, s3key, instructions):
        path = os.path.join(os.getcwd(), f"{self.collection_id}")
        if not os.path.exists(path):
            os.mkdir(path)

        mediajson_path = os.path.join(path, "media_json")
        if not os.path.exists(mediajson_path):
            os.mkdir(mediajson_path)

        filename = os.path.join(mediajson_path, f"{instructions['calisphere-id']}")
        f = open(filename, "w")
        media_json = json.dumps(instructions)
        f.write(media_json)