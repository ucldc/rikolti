from .oai_mapper import OaiRecord, OaiVernacular


class TvAcademyRecord(OaiRecord):

    def UCLDC_map(self):
        return {
            'is_shown_at': self.first_string_in_field('identifier'),
            'is_shown_by': self.first_string_in_field('relation')
        }


class TvAcademyVernacular(OaiVernacular):
    record_cls = TvAcademyRecord
