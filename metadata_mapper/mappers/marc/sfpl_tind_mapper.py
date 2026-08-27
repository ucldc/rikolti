from .tind_mapper import TindRecord, TindValidator, TindVernacular


class SfplTindRecord(TindRecord):
    def to_UCLDC(self):
        self.legacy_couch_db_id = (
            f"{self.collection_id}--{self.source_metadata.get('id')}"
        )
        return super().to_UCLDC()

    def UCLDC_map(self):
        return {
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "source": self.get_marc_data_fields(["524", ["2"]]),
        }

    def map_is_shown_at(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return "https://digitalsf.org/record/" + field_001

    def map_is_shown_by(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return (
                "https://digitalsf.org/nanna/thumbnail/v2/" + field_001 + "?redirect=1"
            )

    def map_relation(self) -> list:
        field_range = [str(i) for i in list(range(760, 788)) + [982]]  # 760-787 + 982

        self.get_marc_data_fields(field_range)

    def map_subject(self):
        fields = [
            str(i)
            for i in [600, 630, 650, 651]
            + list(range(610, 620))
            + list(range(653, 659))
            + [690, 691]
            + list(range(693, 700))
        ]
        return [
            {"name": s}
            for s in self.get_marc_data_fields(fields, ["2"], exclude_subfields=True)
        ]


class SfplTindVernacular(TindVernacular):
    record_cls = SfplTindRecord
    validator = TindValidator
