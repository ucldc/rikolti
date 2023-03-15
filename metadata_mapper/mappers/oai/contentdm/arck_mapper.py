from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular


class ArckRecord(ContentdmRecord):
    """
    TODO: Analysis of the 11 collections to see if the `map_is_shown_by()` logic can be simplified.
    """

    identifier_match = "view/ARCK3D"

    def UCLDC_map(self):
        return {
            "source": self.source_metadata.get("source")
        }

    def map_is_shown_by(self):
        values = self.source_metadata.get("relation")
        if not values:
            return

        candidates = [v for v in values if "/thumbnail" in v]
        if not candidates:
            return

        return candidates[-1]

    def map_relation(self):
        if self.map_is_shown_by():
            return

        return self.source_metadata.get("relation")


class ArckVernacular(ContentdmVernacular):
    record_cls = ArckRecord
