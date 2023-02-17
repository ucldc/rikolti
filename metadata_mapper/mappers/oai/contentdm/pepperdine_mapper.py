from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular


class PepperdineRecord(ContentdmRecord):

    def UCLDC_map(self):
        return {
            'source': self.source_metadata.get('source')
        }

    def map_is_shown_by(self):
        """
        Does not exclude types containing "sound" like the parent class does
        """
        record_type = self.map_type()
        if not record_type:
            return

        return self.get_preview_image_url()


class PepperdineVernacular(ContentdmVernacular):
    record_cls = PepperdineRecord
