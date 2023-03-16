from .oai_mapper import OaiRecord, OaiVernacular


class PsplRecord(OaiRecord):
    """
    TODO: the `is_shown_by` value returns an error message "Error: No 'key' parameter was provided."
    """
    def map_is_shown_by(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        _, record_id = identifier.rsplit(':', 1)
        return "https://collections.accessingthepast.org/cgi-bin/imageserver.pl"\
               f"?oid={record_id}.1.1&width=400&ext=jpg"

    def map_is_shown_at(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        collection_id, record_id = identifier.rsplit(':', 1)
        return f"https://collections.accessingthepast.org/cgi-bin/{collection_id}?a=d&d={record_id}"


class PsplVernacular(OaiVernacular):
    record_cls = PsplRecord
