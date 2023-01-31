from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular, first

class ContentdmOaiDcRecord(OaiRecord):
    @staticmethod
    def __identifier_match(self):
        return "cdm/ref"

    def get_first_match(self, items: dict, match: str):
        return first(self.get_matches(items, match))

    def get_matches(self, items: dict, match: str):
        [item for item in items if match in item]

    def map_is_shown_by(self):
        identifier = self.get_first_match(self.source_metadata.get("identifier", self.__identifier_match()))
        if not identifier:
            return

        base_url, i, j, k, collection_id, l, object_id = identifier.rsplit('/', 6)
        return '/'.join((base_url, 'utils', 'getthumbnail',
                                  'collection', collection_id, 'id', object_id))


    def map_is_shown_at(self):
        return self.get_first_match(self.source_metadata.get("identifier", self._ident_match))

    """This is where we're at"""


    def map_contributor(self):
        self.to_source_resource_with_split('contributor', 'contributor')

    def map_creator(self):
        self.to_source_resource_with_split('creator', 'creator')

    def map_subject(self):
        values = self.split_values('subject')
        subject_objs = [{'name': v} for v in values]
        self.update_source_resource({'subject': subject_objs})

    def map_spatial(self):
        fields = ('coverage', 'spatial')
        self.source_resource_orig_list_to_prop_with_split(fields, 'spatial')

    def map_type(self):
        '''TODO:Funky, but map_type comes after the is_shown_by, should change order
        '''
        self.to_source_resource_with_split('type', 'type')

    def map_language(self):
        self.to_source_resource_with_split('language', 'language')

    def get_url_image_info(self):
        image_info = {'height': 0, 'width': 0}
        ident = self.get_identifier_match('cdm/ref')
        base_url, i, j, k, collid, l, objid = ident.rsplit('/', 6)
        # url_image_info give json data about image for record
        url_image_info = '/'.join((base_url, 'utils', 'ajaxhelper'))
        url_image_info = '{}?CISOROOT={}&CISOPTR={}'.format(url_image_info,
                                                            collid, objid)
        logger.error(url_image_info)
        return url_image_info

    def get_image_info(self):
        '''Return image info for the contentdm object'''
        image_info = {'height': 0, 'width': 0}
        ident = self.get_identifier_match('cdm/ref')
        if ident:
            image_info = requests.get(self.get_url_image_info()).json()[
                'imageinfo']
        return image_info

    def get_larger_preview_image(self):
        # Try to get a bigger image than the thumbnail.
        # Some "text" types have a large image
        image_info = self.get_image_info()
        if image_info['height'] > 0:  # if 0 only thumb available
            # figure scaling
            max_dim = 1024.0
            scale = 100
            if image_info['height'] >= image_info['width']:
                scale = int((max_dim / image_info['height']) * 100)
            else:
                scale = int((max_dim / image_info['width']) * 100)
            scale = 100 if scale > 100 else scale
            thumbnail_url = '{}&action=2&DMHEIGHT=2000&DMWIDTH=2000&DMSCALE={}'.format(
                self.get_url_image_info(), scale)
            self.mapped_data.update({'isShownBy': thumbnail_url})

    def update_mapped_fields(self):
        ''' To run post mapping. For this one, is_shown_by needs
        sourceResource/type'''
        rec_type = self.mapped_data.get('sourceResource',{}).get('type')
        is_sound_object = False
        if isinstance(rec_type, basestring):
            if 'sound' == rec_type.lower():
                is_sound_object = True
        else:  # list type
            for val in rec_type:
                if 'sound' == val.lower():
                    is_sound_object = True
        if not is_sound_object:
            self.get_larger_preview_image()
        else:
            # NOTE: Most contentdm sound objects have bogus placeholder
            # thumbnails, so don't even bother trying to get any image
            if 'isShownBy' in self.mapped_data:
                del self.mapped_data['isShownBy']










    def map_is_shown_at(self) -> Union[str, None]:
        return self.map_identifier()

    def map_is_shown_by(self) -> Union[str, None]:
        if not self.is_image_type():
            return

        url: Union[str, None] = self.map_identifier()

        return f"{url.replace('items', 'thumbs')}?gallery=preview" if url else None

    def map_description(self) -> Union[str, None]:
        if 'description' not in self.source_metadata:
            return

        return [d for d in self.source_metadata.get('description') if 'thumbnail' not in d]

    def is_image_type(self) -> bool:
        if "type" not in self.source_metadata:
            return False

        type: list[str] = self.source_metadata.get("type", [])

        return type and type[0].lower() == "image"

    def map_identifier(self) -> Union[str, None]:
        if "identifier" not in self.source_metadata:
            return

        identifiers = [i for i in self.source_metadata.get('identifier') if "context" not in i]
        return identifiers[0] if identifiers else None

class ContentdmOaiDcVernacular(OaiVernacular):
    record_cls = ContentdmOaiDcRecord
