from .tind_mapper import TindRecord, TindValidator, TindVernacular


class UcbTindRecord(TindRecord):
    def UCLDC_map(self):
        return {
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by
        }

    def map_is_shown_at(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return "https://digicoll.lib.berkeley.edu/record/" + field_001

    def map_is_shown_by(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return ("https://digicoll.lib.berkeley.edu/nanna/thumbnail/v2/" +
                    field_001 + "?redirect=1")

class UcbTindVernacular(TindVernacular):
    record_cls = UcbTindRecord
    validator = TindValidator
