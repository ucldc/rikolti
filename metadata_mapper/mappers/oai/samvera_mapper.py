from .oai_mapper import OaiRecord, OaiVernacular


class SamveraRecord(OaiRecord):

    def UCLDC_map(self):
        return {
            'is_shown_at': self.source_metadata.get('isShownAt'),
            'is_shown_by': self.source_metadata.get('isShownBy')
        }


class SamveraVernacular(OaiVernacular):
    record_cls = SamveraRecord
