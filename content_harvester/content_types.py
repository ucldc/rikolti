from urllib.parse import urlparse

class Media(object):
    def __init__(self, content_src):
        self.src_url = content_src.get('url')

        url_path = urlparse(content_src.get('url', '')).path
        path_parts = [p for p in url_path.split('/') if p]
        basename = path_parts[-1] if path_parts else None

        self.src_filename = content_src.get('filename', basename)
        self.src_mime_type = content_src.get('mimetype')
        self.src_nuxeo_type = content_src.get('nuxeo_type')
        self.tmp_filepath = f"/tmp/{self.src_filename}"

    def __bool__(self):
        return bool(self.src_url)

class Thumbnail(object):
    def __init__(self, content_src):
        self.src_url = content_src.get('url')

        url_path = urlparse(content_src.get('url', '')).path
        path_parts = [p for p in url_path.split('/') if p]
        basename = path_parts[-1] if path_parts else None

        self.src_filename = content_src.get('filename', basename)
        self.src_mime_type = content_src.get('mimetype', 'image/jpeg')
        self.tmp_filepath = f"/tmp/{self.src_filename}"
    
    def __bool__(self):
        return bool(self.src_url)


class UnsupportedMimetype(Exception):
    pass


def check_media_mimetype(mimetype: str) -> None:
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



