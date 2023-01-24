from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import first, last


class UpOaiDcRecord(OaiRecord):
    def map_is_shown_by(self):
        if 'description' not in self.source_metadata:
            return

        return first([u.replace('thumbnail', 'preview')
                      for u in filter(None, self.source_metadata.get('description'))])

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        return last([i for i in filter(None, self.source_metadata.get('identifier'))
                     if 'scholarlycommons.pacific.edu' in i
                     and 'viewcontent' not in i])

    def map_description(self):
        if 'description' not in self.source_metadata:
            return

        return [d for d in filter(None, self.source_metadata.get('description')) if 'thumbnail.jpg' not in d]


class UpOaiDcVernacular(OaiVernacular):
    record_cls = UpOaiDcRecord
