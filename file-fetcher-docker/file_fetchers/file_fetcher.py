import sys

""" fetch file(s) for a given object. Or should this be for a collection?
    ultimately, we want flexibility. Usually will kick it off for a collection,
    but sometimes we will want the option to fun for just one object.
    
    what should this parent class do? 
    - take a metadata record - in the case of nuxeo, is this the media.json?
    - figure out the source url??????
    - stash content file on s3

    return: {
    ‘calisphere-id’: <nuxeo id, calisphere id>, 
    ‘href’: <original URL>, 
    ‘format’: <format>
}


"""

class FileFetcher(object):
    def __init__(self, calisphere_id):
        self.calisphere_id = 'dc38be22-3f85-4e32-9e5e-a2f3763ccfb0'

        # parse the media.json file for goodies
        self.mime_type = 'application/pdf'
        self.source_url = 'https://nuxeo.cdlib.org/Nuxeo/nxfile/default/dc38be22-3f85-4e32-9e5e-a2f3763ccfb0/file:content/curivsc_ms272_001_005.pdf'
        self.filename = 'curivsc_ms272_001_005.pdf'

    def list_source_urls(self, metadata):
        pass
        # this will be subclassed for each data source



    def fetch_to_s3(self, source_url):
        # this should work for most sources, unless we have a really funky API
        # check to see if file already exists on s3
        # stream file to s3

        """
        return: {
            ‘calisphere-id’: <nuxeo id, calisphere id>, 
            ‘href’: <original URL>, 
            ‘format’: <format>
        }
        """

    def get_records(self):
        pass

    def increment(self):
        pass

def main():
    calisphere_id = 'dc38be22-3f85-4e32-9e5e-a2f3763ccfb0'
    filefetcher = FileFetcher(calisphere_id)
    filefetcher.fetch_content_file()

if __name__ == "__main__":
    sys.exit(main())

