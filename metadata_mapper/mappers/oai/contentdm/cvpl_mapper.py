from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular


class CvplRecord(ContentdmRecord):
    identifier_match = "//archives.chulavistalibrary.com"

    def get_larger_preview_image_url(self):
        identifier = self.get_matching_identifier(last=True)
        if not identifier:
            return

        base_url, _, collection_comma_object = identifier.rsplit('/', 2)
        collection_id, object_id = collection_comma_object.split(',')
        return f"{base_url}/cgi-bin/getimage.exe?DMSCALE=20&CISOROOT=/"\
               f"{collection_id}&CISOPTR={object_id}"

    def map_is_shown_at(self):
        return self.get_matching_identifier(last=True)


class CvplVernacular(ContentdmVernacular):
    record_cls = CvplRecord
