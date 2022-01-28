import os

''' just creating a skeleton of this for now to think through
    how this is gonna work
'''

class OACFileFetcher(object):

    def __init__(self, collection_id):
        self.collection_id = collection_id

    def fetch_content_files(self):
        """ fetch content file to store in s3 """
        print(f"fetching content files for collection {self.collection_id=}")

        self.source_urls = self.list_source_urls()
        self.stash_content_files(self.source_urls)

    def list_source_urls(self):
        """ given source metadata, get a list of urls """
        pass

    def stash_content_files(self, source_urls):
        """ hit each source url and stash files if appropriate 
            appropriate == textractable

            what about md5s3stash?
        """
