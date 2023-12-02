import os
from . import derivatives

class UnsupportedMimetype(Exception):
    pass


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


