import sys

'''
ContentFile (calisphere_id)
    get content file location from media.json file
    stash content file on s3
    return: {
‘calisphere-id’: <nuxeo id, calisphere id>, 
‘href’: <original nuxeo URL>, 
‘format’: <format>
}
'''

'''
media.json, for nuxeo files only:
{
    "calisphere-id": 27414--dc38be22-3f85-4e32-9e5e-a2f3763ccfb0,
    "type": "CustomFile",
    "content-file": {
        "mime-type": "application/pdf",
        "url": https://nuxeo.cdlib.org/Nuxeo/nxfile/default/dc38be22-3f85-4e32-9e5e-a2f3763ccfb0/file:content/curivsc_ms272_001_005.pdf,
        "filename": curivsc_ms272_001_005.pdf
    },
    "thumbnail": {
        conversion-type: 'pdf'
    }
}

For non-nuxeo sources, we hit the equivalent of the isShownAt URL. 
Where does this get built?
'''

class NuxeoFileFetcher(object):
    def __init__(self, params):
        self.calisphere_id = 'dc38be22-3f85-4e32-9e5e-a2f3763ccfb0'

        # parse the media.json file for goodies
        self.mime_type = 'application/pdf'
        self.source_url = 'https://nuxeo.cdlib.org/Nuxeo/nxfile/default/dc38be22-3f85-4e32-9e5e-a2f3763ccfb0/file:content/curivsc_ms272_001_005.pdf'
        self.filename = 'curivsc_ms272_001_005.pdf'
        

    def fetch_content_file(self):
        """ Fetch content file to store in s3 """

        print(f"fetching content file for: {self.calisphere_id}")

class NuxeoThumbnailFetcher(object):

    def __init__(self, params):
        pass

    def fetch_thumbnail(self):
        pass


def main():
    calisphere_id = 'dc38be22-3f85-4e32-9e5e-a2f3763ccfb0'
    filefetcher = NuxeoFileFetcher(calisphere_id)
    filefetcher.fetch_content_file()

if __name__ == "__main__":
    sys.exit(main())
