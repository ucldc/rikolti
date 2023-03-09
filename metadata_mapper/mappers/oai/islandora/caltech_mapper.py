from ..islandora_mapper import IslandoraRecord, IslandoraVernacular
import requests

class CaltechRecord(IslandoraRecord):
    """TODO: the concept of restriction is novel in Rikolti; it appears to prevent mapping a record altogether
    TODO: This mapper hasn't been tested yet (caltech endpoints returning errors)
    """
    identifier_match = "library.caltech.edu"

    def map_is_shown_at(self):
        identifiers = [i for i in filter(None, self.source_metadata.get('identifier'))
                       if i and self.identifier_match in i]

        if not identifiers:
            return

        return identifiers[-1]

    def map_is_shown_by(self):
        # Change URL from 'TN' to 'JPG' for larger versions of image objects & test to make sure the link resolves
        thumb_url = self.source_metadata.get('identifier.thumbnail')
        if not thumb_url:
            return

        thumb_url = thumb_url[0]

        if 'type' in self.source_metadata and any(s in self.source_metadata.get('type')
                                                  for s in ['StillImage', 'image']):
            jpg_url = thumb_url.replace("/TN/", "/JPG/")

            try:
                request = requests.get(jpg_url)
            except request.RequestException:
                pass
            if request.status_code == 200:
                thumb_url = jpg_url
        return thumb_url


class CaltechVernacular(IslandoraVernacular):
    record_cls = CaltechRecord

    def skip(self, record):
        title = record.get("title")

        if not title:
            return False

        if not isinstance(title, list):
            title = [title]

        if any(t.startswith(("Finding Aid", "PBM_", "DAG_", "DAGB_")) for t in title):
            return True

        return False
