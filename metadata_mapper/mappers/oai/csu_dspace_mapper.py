import re
import urllib

from .oai_mapper import OaiRecord, OaiVernacular


class CsuDspaceRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            'language': self.source_metadata.get('languageTerm'),
            'subject': self.source_metadata.get('topic'),
            'date': self.source_metadata.get('dateIssued'),
            'creator': self.source_metadata.get('namePart'),
            'type': self.collate_fields(['type', 'genre'])
        }

    def map_is_shown_by(self):
        if not self.source_metadata.get('originalName'):
            return

        extensions = ['.txt', '.doc', '.tif', '.wav', '.mp4']

        filenames = [f for f in filter(None, self.source_metadata.get('originalName'))
                     if not any([x in f.lower() for x in extensions])]

        thumbnail_url = None
        for filename in filenames:
            thumbnail_url = f"{self.get_baseurl()}/bitstream/handle/"\
                            f"{self.get_handle()}/{urllib.parse.quote(filename)}"

            if filename.endswith('.pdf'):
                thumbnail_url = thumbnail_url + '.jpg'

        return thumbnail_url

    def map_is_shown_at(self):
        handle = self.get_handle()
        if not handle:
            return

        return f"{self.get_baseurl()}/handle/{handle}"

    def get_handle(self):
        values = [re.sub(r'(https?:\/\/hdl\.handle\.net\/)', '', h)
                  for h in filter(None, self.source_metadata.get('identifier'))
                  if 'hdl.handle.net' in h]

        if not values:
            return

        return values[-1]

    def get_baseurl(self):
        return 'https://dspace.calstate.edu'


class CsuDspaceVernacular(OaiVernacular):
    record_cls = CsuDspaceRecord
