from .tind_mapper import TindRecord, TindValidator, TindVernacular

class SfplTindRecord(TindRecord):
    def to_UCLDC(self):
        self.legacy_couch_db_id = f"{self.collection_id}--{self.source_metadata.get('id')}"
        return super().to_UCLDC()

    def UCLDC_map(self):
        return {
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by
        }
    
    def map_is_shown_at(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return "https://digitalsf.org/record/" + field_001
        
    def map_is_shown_by(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return ("https://digitalsf.org/nanna/thumbnail/v2/" +
                    field_001 + "?redirect=1")

class SfplTindVernacular(TindVernacular):
    record_cls = SfplTindRecord
    validator = TindValidator
