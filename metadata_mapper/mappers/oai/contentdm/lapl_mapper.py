from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular


class LaplRecord(ContentdmRecord):
    def map_is_shown_at(self):
        if "identifier" not in self.source_metadata:
            return

        is_shown_at = [f"https://tessa.lapl.org/cdm{i.split('cdm')[1]}"
                       for i in self.source_metadata.get('identifier')
                       if self.identifier_match in i]

        if is_shown_at:
            return is_shown_at[-1]


class LaplVernacular(ContentdmVernacular):
    record_cls = LaplRecord
