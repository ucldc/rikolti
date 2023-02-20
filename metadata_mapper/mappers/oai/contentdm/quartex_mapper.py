from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular


class QuartexRecord(ContentdmRecord):
    def map_is_shown_at(self):
        if "identifier" not in self.source_metadata:
            return

        is_shown_at = [i for i in self.source_metadata.get("identifier")
                       if "documents/detail" in i]

        if is_shown_at:
            return is_shown_at[-1]

    def map_is_shown_by(self):
        """
        TODO: determine if the following comment from the legacy mapper needs to be implemented (code both
            with and without follows):

        # Grab the image URL from identifier values and switch out Size2 for Size4 (largest possible)"""

        values = self.source_metadata.get("identifier")
        if not values:
            return

        candidates = [v for v in values if "thumbnails/preview" in v]
        if not candidates:
            return

        return candidates[-1]
        # OR do the replacement that the legacy mapper describes but doesn't implement
        # return candidates[-1].replace("Size2", "Size4")


class QuartexVernacular(ContentdmVernacular):
    record_cls = QuartexRecord
