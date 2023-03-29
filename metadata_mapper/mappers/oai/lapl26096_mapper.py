from .oai_mapper import OaiRecord, OaiVernacular


class Lapl26096Record(OaiRecord):
    BASE_URL = "https://rescarta.lapl.org/ResCarta-Web/"
    IMAGE_URL_FORMAT = "%1sservlet/RcWebThumbnail?obj_type=SERIAL_MONOGRAPH&pg_idx=0&obj_id=%2s"

    def map_is_shown_at(self):
        identifier_path = self.get_identifier_path()
        if not identifier_path:
            return

        return f"{self.BASE_URL}{identifier_path}"

    def map_is_shown_by(self):
        identifier_path = self.get_identifier_path()
        if not identifier_path:
            return

        object_id = identifier_path[identifier_path.index('=') + 1:]
        object_thumb_id = object_id.split('/', 1)[1]
        return self.IMAGE_URL_FORMAT % (self.BASE_URL, object_thumb_id)

    def get_identifier_path(self):
        identifier = self.source_metadata.get('identifier')

        if not identifier or 'jsp' not in identifier[0]:
            return ''

        return identifier[0][identifier.index('jsp'):]


class Lapl26096Vernacular(OaiVernacular):
    record_cls = Lapl26096Record
