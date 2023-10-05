from .oai_mapper import OaiRecord, OaiVernacular


class TvAcademyRecord(OaiRecord):

    def UCLDC_map(self) -> dict:
        return {
            'isShownAt': self.first_string_in_field('identifier'),
            'isShownBy': self.first_string_in_field('relation'),
            "relation": None
        }


class TvAcademyVernacular(OaiVernacular):
    record_cls = TvAcademyRecord
