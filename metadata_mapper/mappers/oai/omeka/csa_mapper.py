from typing import Union

from ..omeka_mapper import OmekaRecord, OmekaVernacular


class CsaRecord(OmekaRecord):
    def UCLDC_map(self):
        return {
            'spatial': self.map_spatial
        }

    def map_is_shown_by(self):
        identifiers = [i for i
                       in filter(None, self.source_metadata.get('identifier', []))
                       if 'files/original' in i]
        return identifiers[0] if identifiers else None

    def map_spatial(self) -> Union[list[str], None]:
        spatial = self.collate_fields(["coverage", "spatial"])()
        spatial = [s for s in spatial if s]
        split_spatial = []
        for value in spatial:
            split_spatial.extend(value.split(';'))

        return [val.strip() for val in split_spatial if val]


class CsaVernacular(OmekaVernacular):
    record_cls = CsaRecord
