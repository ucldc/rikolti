from .oai_mapper import OaiRecord, OaiVernacular


class PsplRecord(OaiRecord):
    def map_is_shown_by(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        _, record_id = identifier.rsplit(':', 1)
        return f"https://collections.accessingthepast.org/?a=is&oid={record_id}.1.1"\
               "&type=pagethumbnailimage&width=200"

    def map_is_shown_at(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        collection_id, record_id = identifier.rsplit(':', 1)
        return f"https://collections.accessingthepast.org/cgi-bin/{collection_id}?a=d&d={record_id}"


class PsplVernacular(OaiVernacular):
    record_cls = PsplRecord
