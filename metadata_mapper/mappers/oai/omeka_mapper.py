from .oai_mapper import OaiRecord, OaiVernacular
import requests


class OmekaRecord(OaiRecord):
    """
    TODO: this mapper's `map_is_shown_by` makes a request to generate value in some cases
    """

    def map_is_shown_at(self):
        identifiers = [i for i in filter(None, self.source_metadata.get('identifier')) if 'items/show' in i]

        # TODO: This is true to the legacy mapper, but some analysis may be in order
        if identifiers:
            return identifiers[-1]

    def map_is_shown_by(self):
        """
        If this logic changes, those changes may need to be reflected in NothumbVernacular.skip()
        """
        identifiers = filter(None, self.source_metadata.get('identifier'))
        for i in identifiers:
            if 's3.amazonaws.com/omeka-net' in i:
                return i
            elif '/files/thumbnails/' in i:
                return i
            elif '/files/original/' in i:
                if i.rsplit('.', 1)[1] == 'jpg':
                    return i
                else:
                    thumb_url = i.replace("/original/", "/thumbnails/")
                    thumb_url = thumb_url.rsplit('.', 1)[0] + '.jpg'
                    request = requests.get(thumb_url)
                    if request.status_code == 200:
                        return thumb_url
                    else:
                        return i

    def map_identifier(self):
        if 'identifier' not in self.source_metadata:
            return

        identifiers = self.provider_data_source.get('identifier')
        if isinstance(identifiers, (str, bytes)):
            return identifiers if 'islandora' not in identifiers else None

        filtered_identifiers = []
        for i in filter(None, identifiers):
            if "s3.amazonaws.com/omeka-net/" in i:
                continue
            if "files/original" in i:
                continue
            if "items/show" in i:
                continue
            filtered_identifiers.append(i)
        if filtered_identifiers:
            return filtered_identifiers

class OmekaVernacular(OaiVernacular):
    record_cls = OmekaRecord
