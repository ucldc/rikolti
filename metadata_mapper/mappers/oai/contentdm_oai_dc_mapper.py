from typing import Union
import itertools
import requests

from .oai_mapper import OaiRecord, OaiVernacular, first, last

class ContentdmOaiDcRecord(OaiRecord):
    def UCLDC_map(self):
        {
            "contributor": self.map_contributor(),
            "creator": self.map_creator(),
            "spatial": self.map_spatial(),
            "type": self.map_type(),
            "language": self.map_language()
        }

    @staticmethod
    def __identifier_match():
        return "cdm/ref"

    @staticmethod
    def __split_string():
        return ";"

    def get_first_match(self, items: list, match: str):
        """static

        Given a list and a match string, returns the first match
        """
        return first(self.get_matches(items, match))

    def get_last_match(self, items: list, match: str):
        return last(self.get_matches(items, match))

    def get_matches(self, items: list, match: str):
        """static

        Given a ist and a match string, returns those dictionary items containing the string
        """
        [item for item in items if match in item]

    def flatten_list(self, items: list):
        """static

        Given a list, flattens and strips the list items
        """
        return list([s.strip() for s in itertools.chain.from_iterable(items)])

    def split_and_flatten(self, items: list, split: str):
        """static

        Given a list of strings or nested lists, splits the values on the split string then flattens
        """
        return self.flatten_list([c.split(split) for c in filter(None, items)])

    def ensure_list(self, item: Union[str, list]):
        """static

        Given a list or string, ensures we have a list
        """
        return [item] if isinstance(item, str) else item

    def map_is_shown_at(self):
        return self.get_last_match(self.source_metadata.get("identifier", self.__identifier_match()))

    def map_contributor(self):
        if "contributor" not in self.source_metadata:
            return

        return self.split_and_flatten(self.ensure_list(self.source_metadata.get("contributor")), self.__split_string())

    def map_creator(self):
        if "creator" not in self.source_metadata:
            return

        return self.split_and_flatten(self.ensure_list(self.source_metadata.get("creator")), self.__split_string())

    def map_subject(self):
        if "subject" not in self.source_metadata:
            return

        values = self.split_and_flatten(self.ensure_list(self.source_metadata.get("subject")), self.__split_string())
        return [{'name': v} for v in values]

    def map_spatial(self):
        values = self.source_metadata_values(["coverage", "spatial"])
        if not values:
            return

        return self.split_and_flatten(values)

    def map_type(self):
        return self.split_and_flatten(self.source_metadata.get("type"), self.__split_string())

    def map_language(self):
        return self.split_and_flatten(self.source_metadata.get("language"), self.__split_string())

    def map_is_shown_by(self):
        """
        This was originally a "post-map" function. but really, this is seems like it should be map_is_shown_by()

        Comment from calisphere function:
        To run post mapping. For this one, is_shown_by needs
        sourceResource/type"""
        record_type = self.map_type()
        if "sound" in [t.lower() for t in self.ensure_list(record_type)]:
            return None

        return self.get_preview_image_url()

    def get_preview_image_url(self):
        """
        see get_larger_preview_image(), below, as it is mapped to isShownBy as well

        """
        larger_preview_image = self.get_larger_preview_image_url()
        if larger_preview_image:
            return larger_preview_image

        parts = self.get_identifier_parts()
        return '/'.join((parts["base_url"], 'utils', 'getthumbnail',
                         'collection', parts["collection_id"], 'id', parts["object_id"]))

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
        return '{}&action=2&DMHEIGHT=2000&DMWIDTH=2000&DMSCALE={}'.format(self.get_url_image_info(), scale)

    def get_image_info(self):
        """
        This seems wrong, shouldn't an enrichment handle image-related tasks?
        """
        image_info = {'height': 0, 'width': 0}
        identifier = self.get_first_match(self.source_metadata.get("identifier", self.__identifier_match()))
        if not identifier:
            return image_info

        response = requests.get(self.get_url_image_info()).json()['imageinfo']
        return response if response else image_info

    def get_url_image_info(self):
        parts = self.get_identifier_parts()
        url_image_info = '/'.join((parts["base_url"], 'utils', 'ajaxhelper'))
        return '{}?CISOROOT={}&CISOPTR={}'.format(url_image_info, parts["collection_id"], parts["object_id"])

    def get_identifier_parts(self):
        identifier = self.get_first_match(self.source_metadata.get("identifier", self.__identifier_match()))
        base_url, _, _, _, collection_id, _, object_id = identifier.rsplit('/', 6)
        return {
            "base_url": base_url,
            "collection_id": collection_id,
            "object_id": object_id
        }


class ContentdmOaiDcVernacular(OaiVernacular):
    record_cls = ContentdmOaiDcRecord
