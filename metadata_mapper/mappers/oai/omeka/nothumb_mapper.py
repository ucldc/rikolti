from ..omeka_mapper import OmekaRecord, OmekaVernacular


class NothumbRecord(OmekaRecord):
    pass


class NothumbVernacular(OmekaVernacular):
    record_cls = NothumbRecord

    def skip(self, record):
        """
        This is a lightweight version of OmekaRecord.map_is_shown_by(). Any changes here may need to be
        reflected there.
        """
        return not any([search in identifier
                        for search in ['s3.amazonaws.com/omeka-net', '/files/thumbnails/', '/files/original/']
                        for identifier in filter(None, record.get('identifier'))])
