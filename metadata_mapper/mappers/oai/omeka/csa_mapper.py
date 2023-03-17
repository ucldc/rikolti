from ..omeka_mapper import OmekaRecord, OmekaVernacular


class CsaRecord(OmekaRecord):
    def map_is_shown_by(self):
        identifiers = [i for i in filter(None, self.source_metadata.get('identifier', [])) if 'files/original' in i]
        return identifiers[0] if identifiers else None


class CsaVernacular(OmekaVernacular):
    record_cls = CsaRecord
