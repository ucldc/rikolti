from ..contentdm_oai_dc_mapper import ContentdmOaiDcRecord, ContentdmOaiDcVernacular
from ..oai_mapper import last

class CvplOaiDcRecord(ContentdmOaiDcRecord):
    @staticmethod
    def __identifier_match():
        return "//archives.chulavistalibrary.com"

    def get_larger_preview_image_url(self):
        """
        TODO: look into multiple matches on identifier, like the following function

        """
        identifier = last(self.get_matches(self.source_metadata.get("identifier"), self.__identifier_match()))
        if not identifier:
            return

        base_url, _, collection_comma_object = identifier.rsplit('/', 2)
        collection_id, object_id = collection_comma_object.split(',')
        return ''.join((base_url, '/cgi-bin',
                                 '/getimage.exe?DMSCALE=20&CISOROOT=/', collection_id,
                                 '&CISOPTR=', object_id))


    def map_is_shown_at(self):
        """
        According to the legacy mapper, this match requires both the domain, and the "cdm/ref" value
        from the parent mapper. See: https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/chula_vista_pl_contentdm_oai_dc_mapper.py

        TODO: this requires another pass to see if sibling mappers also require multiple match strings on the identifier

        """
        with_domain = self.get_matches(self.source_metadata.get("identifier"), self.__identifier_match())
        return last(self.get_matches(with_domain, "cdm/ref"))




class CvplOaiDcVernacular(ContentdmOaiDcVernacular):
    record_cls = CvplOaiDcRecord
