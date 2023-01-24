from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import last, collate_values


class YosemiteOaiDcRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            'temporal': None,
            'date': collate_values(
                self.source_metadata_values("available", "created", "dateAccepted", "dateCopyrighted", "dateSubmitted",
                                            "issued", "modified", "valid", "temporal"))
        }

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        return last([i for i in filter(None, self.source_metadata.get('identifier'))
                     if 'npgallery.nps.gov' in i
                     and 'AssetDetail' in i])

    def map_is_shown_by(self):
        if 'identifier' not in self.source_metadata:
            return

        return last([i for i in filter(None, self.source_metadata.get('identifier'))
                     if 'npgallery.nps.gov' in i
                     and i.endswith('/')])


class YosemiteOaiDcVernacular(OaiVernacular):
    record_cls = YosemiteOaiDcRecord
