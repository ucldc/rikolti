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

    def __bool__(self):
        return not self.missing


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

    @classmethod
    def check_mimetype(cls, mimetype):
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


