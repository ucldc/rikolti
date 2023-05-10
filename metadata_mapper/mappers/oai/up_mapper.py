from .oai_mapper import OaiRecord, OaiVernacular


class UpRecord(OaiRecord):
    def map_is_shown_by(self):
        if 'description' not in self.source_metadata:
            return

        values = [u.replace('thumbnail', 'preview')
                  for u in filter(None, self.source_metadata.get('description'))]

        if values:
            return values[0]

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'scholarlycommons.pacific.edu' in i and 'viewcontent' not in i]

        if values:
            return values[-1]

    def map_description(self):
        if 'description' not in self.source_metadata:
            return

        return [d for d in filter(None, self.source_metadata.get('description'))
                if 'thumbnail.jpg' not in d]


class UpVernacular(OaiVernacular):
    record_cls = UpRecord
