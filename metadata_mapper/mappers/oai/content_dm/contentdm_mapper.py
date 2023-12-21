import itertools

import requests

from ..oai_mapper import OaiRecord, OaiVernacular


class ContentdmRecord(OaiRecord):
    identifier_match = "cdm/ref"

    def UCLDC_map(self):
        return {
            "contributor": self.split_and_flatten('contributor'),
            "creator": self.split_and_flatten('creator'),
            "spatial": self.map_spatial,
            "type": self.map_type,
            "language": self.split_and_flatten('language'),
            "subject": self.map_subject
        }

    def map_is_shown_at(self):
        return self.get_matching_identifier(last=True)

    def map_is_shown_by(self):
        """
        This was originally a "post-map" function. but really, this is seems like
        it should be map_is_shown_by()

        Comment from calisphere function:
        To run post mapping. For this one, is_shown_by needs sourceResource/type
        """
        record_type = self.map_type()
        if not record_type:
            return

        record_types = [record_type] if isinstance(record_type, str) else record_type

        if "sound" in [t.lower() for t in record_types]:
            return None

        return self.get_preview_image_url()

    def map_type(self):
        """Used by map_is_shown_by() in this class, so cannot be directly added to
        the mapping
        """
        return self.split_and_flatten('type')()

    def map_spatial(self):
        values = [v for v in self.collate_fields(["coverage", "spatial"])() if v]
        if not values:
            return

        split_values = [c.split(';') for c in filter(None, values)]

        return list([s.strip() for s in itertools.chain.from_iterable(split_values)])

    def map_subject(self):
        subject = self.source_metadata.get('subject')
        if not subject:
            return

        return [{'name': v} for v in self.split_and_flatten('subject')()]

    def get_preview_image_url(self):
        """
        see get_larger_preview_image(), below, as it is mapped to isShownBy as well
        """
        larger_preview_image = self.get_larger_preview_image_url()
        if larger_preview_image:
            return larger_preview_image

        parts = self.get_identifier_parts()
        return f"{parts['base_url']}/utils/getthumbnail/collection/"\
               f"{parts['collection_id']}/id/{parts['object_id']}"

    def get_larger_preview_image_url(self):
        image_info = self.get_image_info()
        if not image_info['height'] > 0:
            return

        max_dim = 1024.0
        if image_info['height'] >= image_info['width']:
            scale = int((max_dim / image_info['height']) * 100)
        else:
            scale = int((max_dim / image_info['width']) * 100)
        scale = 100 if scale > 100 else scale
        return f"{self.get_url_image_info()}&action=2&DMHEIGHT=2000"\
               f"&DMWIDTH=2000&DMSCALE={scale}"

    def get_image_info(self):
        """
        TODO: for Amy or Barbara: alternative strategy for making another request
        """
        image_info = {'height': 0, 'width': 0}
        identifier = self.get_matching_identifier()
        if not identifier:
            return image_info

        image_info_url = self.get_url_image_info()
        if image_info_url:
            resp = requests.get(image_info_url)
            resp.raise_for_status()
            if resp.json().get('imageinfo'):
                image_info = resp.json().get('imageinfo')

        return image_info

    def get_url_image_info(self):
        parts = self.get_identifier_parts()
        if not parts:
            return

        url_image_info = '/'.join((parts["base_url"], 'utils', 'ajaxhelper'))
        return f"{url_image_info}?CISOROOT={parts['collection_id']}"\
               f"&CISOPTR={parts['object_id']}"

    def get_identifier_parts(self):
        identifier = self.get_matching_identifier()
        if not identifier:
            return

        base_url, _, _, _, collection_id, _, object_id = identifier.rsplit('/', 6)
        return {
            "base_url": base_url,
            "collection_id": collection_id,
            "object_id": object_id
        }

    def get_matching_identifier(self, last=False):
        """Gets a matching identifier, defaults to the first one, pass last=True to
        get the last one
        """
        identifiers = [i for i in self.source_metadata.get("identifier", [])
                       if i and self.identifier_match in i]
        if not identifiers:
            return

        if last:
            return identifiers[-1]
        return identifiers[0]


class ContentdmVernacular(OaiVernacular):
    record_cls = ContentdmRecord
