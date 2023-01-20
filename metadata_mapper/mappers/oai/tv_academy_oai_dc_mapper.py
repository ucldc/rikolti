from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import first


class TvAcademyOaiDcRecord(OaiRecord):
    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        return first(self.source_metadata.get('identifier'))

    def map_is_shown_by(self):
        if 'relation' not in self.source_metadata:
            return

        return first(self.source_metadata.get('relation'))


class TvAcademyOaiDcVernacular(OaiVernacular):
    record_cls = TvAcademyOaiDcRecord
