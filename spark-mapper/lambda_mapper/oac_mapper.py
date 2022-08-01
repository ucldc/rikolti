import os
import re
from utils import exists, getprop, iterify

URL_OAC_CONTENT_BASE = os.environ.get(
    'URL_OAC_CONTENT_BASE', 'http://content.cdlib.org')

# collection 25496 has coverage values like A0800 & A1000
# drop these
Anum_re = re.compile('A\d\d\d\d')


class OAC_DCMapper(object):
    def __init__(self, provider_data):
        self.provider_data = provider_data
        self.mapped_data = {"sourceResource": {}}

    def map(self):
        provider_data = self.provider_data

        id = provider_data.get("id", "")
        self.mapped_data.update({
            "id": id,
            "_id": provider_data.get("_id"),
            "@id": f"http://ucldc.cdlib.org/api/items/{id}",
            "originalRecord": provider_data.get("originalRecord"),
            "ingestDate": provider_data.get("ingestDate"), 
            "ingestType": provider_data.get("ingestType"), 
            "ingestionSequence": provider_data.get("ingestionSequence"),
            # This is set in select-oac-id but must be added to mapped data
            'isShownAt': provider_data.get('isShownAt', None),
            'provider': provider_data.get('provider'),
            'dataProvider': provider_data.get(
                'originalRecord', {}).get('collection'),
            'isShownBy': self.get_best_oac_image(),
            'item_count': self.map_item_count(),
        })

        self.remove_if_none([
            "provider",
            "dataProvider",
            "isShownBy",
            "item_count",
        ])

        """Maps the mapped_data sourceResource fields."""
        self.mapped_data["sourceResource"].update({
            "contributor": self.get_vals("contributor"),
            "creator": self.get_vals("creator"),
            "extent": self.get_vals("extent"),
            "language": self.get_vals("language"),
            "publisher": self.get_vals("publisher"),
            "provenance": self.get_vals("provenance"),

            'description': self.collate(
                ("abstract", "description", "tableOfContents")),
            'identifier': self.collate(
                ("bibliographicCitation", "identifier")),
            'rights': self.collate(("accessRights", "rights")),

            "date": self.get_vals(
                "date", suppress_attribs={'q': 'dcterms:dateCopyrighted'}),
            "format": self.get_vals("format", suppress_attribs={'q': 'x'}),
            "title": self.get_vals(
                "title", suppress_attribs={'q': 'alternative'}),
            "type": self.get_vals(
                "type", suppress_attribs={'q': 'genreform'}),
            'subject': self.map_subject(),

            'copyrightDate': self.map_specific(
                'date', 'dcterms:dateCopyrighted'),
            'alternativeTitle': self.map_specific('title', 'alternative'),
            'genre': self.map_specific('type', 'genreform'),

            "stateLocatedIn": [{"name": "California"}],
            'spatial': self.map_spatial(),
            'temporal': self.map_temporal(),
        })
            
        self.remove_if_empty_list([
            "copyrightDate", 
            "alternativeTitle", 
            "genre", 
            "description", 
            "identifier", 
            "rights",
            "spatial",
            "temporal"
        ])
        return self.mapped_data

    def remove_if_none(self, fields):
        for field in fields:
            if self.mapped_data[field] is None:
                self.mapped_data.pop(field)

    def remove_if_empty_list(self, fields):
        for field in fields:
            if self.mapped_data["sourceResource"][field] == []:
                self.mapped_data["sourceResource"].pop(field)

    def get_vals(self, provider_prop, suppress_attribs={}):
        '''Return a list of string values take from the OAC type
        original record (each value is {'text':<val>, 'attrib':<val>} object)
        '''
        values = []
        if exists(self.provider_data, provider_prop):
            for x in self.provider_data[provider_prop]:
                try:
                    value = x['text']
                except KeyError:
                    # not an elementtree type data value
                    values.append(x)
                    continue
                if not x['attrib']:
                    values.append(x['text'])
                else:
                    suppress = False
                    for attrib, attval in x['attrib'].items():
                        if attval in suppress_attribs.get(attrib, []):
                            suppress = True
                            break
                    if not suppress:
                        values.append(x['text'])
        return values

    def collate(self, original_fields):
        '''Override to handle elements which are dictionaries of format
        {'attrib': {}, 'text':"string value of element"}

        for a list of fields in the providers original data, append the
        values into a single sourceResource field
        '''
        values = []
        for field in original_fields:
            if exists(self.provider_data, field):
                values.extend(
                    self.get_vals(field))
        return values

    def map_specific(self, src_prop, specify):
        provider_data = self.provider_data.get(src_prop, None)
        values = []
        if provider_data:
            values = [
                d.get('text') for d in provider_data
                if d.get('attrib') if d.get('attrib',{}).get('q') == specify
            ]
        return values

    def get_best_oac_image(self):
        '''From the list of images, choose the largest one'''
        best_image = None
        if 'originalRecord' in self.provider_data:  # guard weird input
            dim = 0
            # 'thumbnail' might be represented different in xmltodict 
            # vs. the custom fetching mark was doing
            thumb = self.provider_data.get('originalRecord', {}).get('thumbnail', None)
            if thumb:
                if 'src' in thumb:
                    dim = max(int(thumb.get('X')), int(thumb.get('Y')))
                    best_image = thumb.get('src')
            # 'reference-image' might be represented differently in xmltodict
            # vs. the custom fetching mark was doing
            ref_images = self.provider_data.get('originalRecord', {}).get(
                'reference-image', [])
            if type(ref_images) == dict:
                ref_images = [ref_images]
            for obj in ref_images:
                if max(int(obj.get('X')), int(obj.get('Y'))) > dim:
                    dim = max(int(obj.get('X')), int(obj.get('Y')))
                    best_image = obj.get('src')
            if best_image and not best_image.startswith('http'):
                best_image = '/'.join((URL_OAC_CONTENT_BASE, best_image))
        return best_image

    def map_item_count(self):
        '''Use reference-image-count value to determine compound objects.
        NOTE: value is not always accurate so only determines complex (-1)
        or not complex (no item_count value)
        '''
        item_count = None
        image_count = 0
        if 'originalRecord' in self.provider_data:  # guard weird input
            ref_image_count = self.provider_data.get('originalRecord',{}).get(
                'reference-image-count')
            if ref_image_count:
                image_count = ref_image_count[0]['text']
            if image_count > "1":
                item_count = "-1"
        return item_count

    def map_spatial(self):
        coverage = []
        if 'originalRecord' in self.provider_data:  # guard weird input
            if 'coverage' in self.provider_data.get('originalRecord'):
                coverage_data = iterify(
                    getprop(self.provider_data.get('originalRecord'), "coverage"))
                # remove arks from data
                # and move the "text" value to
                for c in coverage_data:
                    if (not isinstance(c, basestring) and
                            not c.get('text').startswith('ark:')):
                        if 'q' in c.get('attrib', {}) and 'temporal' not in c.get('attrib',{}).get('q'):
                            coverage.append(c.get('text'))
                        if 'q' not in c.get('attrib', {}) and c.get('attrib', {}) is not None and not Anum_re.match(c.get('text')):
                            coverage.append(c.get('text'))
        return coverage

    def map_temporal(self):
        temporal = []
        if 'originalRecord' in self.provider_data:  # guard weird input
            if 'coverage' in self.provider_data.get('originalRecord'):
                time_data = iterify(
                    getprop(self.provider_data.get('originalRecord'), "coverage"))
                for t in time_data:
                    if 'q' in t.get('attrib', {}) and 'temporal' in t.get('attrib',{}).get('q'):
                        temporal.append(t.get('text'))
        return temporal

    def map_subject(self):
        subject_values = self.get_vals(
            "subject", suppress_attribs={'q': 'series'})
        subject_objs = [{'name': s} for s in subject_values]
        return subject_objs
