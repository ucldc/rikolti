import re
# import lxml
from xml.etree import ElementTree as ET
from collections import defaultdict
from ..mapper import Vernacular, Record
from ..utils import exists, getprop, iterify


class OacRecord(Record):

    def to_UCLDC(self):
        mapped_data = {}

        id = self.pre_mapped_data.get("id", "")
        mapped_data.update({
            "id": id,
            "originalRecord": self.source_metadata,
            # This is set in select-oac-id but must be added to mapped data
            'isShownAt': self.pre_mapped_data.get('isShownAt', None),
            'isShownBy': self.get_best_oac_image(),
        })

        if mapped_data["isShownBy"] is None:
            mapped_data.pop("isShownBy")

        """Maps the mapped_data sourceResource fields."""
        mapped_data.update({
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

        mapped_data = self.remove_if_empty_list([
            "copyrightDate",
            "alternativeTitle",
            "genre",
            "description",
            "identifier",
            "rights",
            "spatial",
            "temporal"
        ], mapped_data)

        self.mapped_data = mapped_data
        return self

    def map_is_shown_at(self):
        return super().map_is_shown_at()

    def map_is_shown_by(self):
        return super().map_is_shown_by()

    def remove_if_empty_list(self, fields, mapped_data):
        for field in fields:
            if mapped_data[field] == []:
                mapped_data.pop(field)
        return mapped_data

    def get_vals(self, provider_prop, suppress_attribs={}):
        '''Return a list of string values take from the OAC type
        original record (each value is {'text':<val>, 'attrib':<val>} object)
        '''
        values = []
        if exists(self.source_metadata, provider_prop):
            for x in self.source_metadata[provider_prop]:
                if not x.get('text'):
                    # not an elementtree type data value
                    values.append(x)
                    continue
                if not x.get('attrib'):
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
            if exists(self.source_metadata, field):
                values.extend(
                    self.get_vals(field))
        return values

    def map_specific(self, src_prop, specify):
        provider_data = self.source_metadata.get(src_prop, None)
        values = []
        if provider_data:
            values = [
                d.get('text') for d in provider_data
                if d.get('attrib') if d.get('attrib', {}).get('q') == specify
            ]
        return values

    def get_best_oac_image(self):
        '''From the list of images, choose the largest one'''
        best_image = None
        if 'originalRecord' in self.source_metadata:  # guard weird input
            dim = 0
            # 'thumbnail' might be represented different in xmltodict
            # vs. the custom fetching mark was doing
            thumb = self.source_metadata.get(
                'originalRecord', {}).get('thumbnail', None)
            if thumb:
                if 'src' in thumb:
                    dim = max(int(thumb.get('X')), int(thumb.get('Y')))
                    best_image = thumb.get('src')
            # 'reference-image' might be represented differently in xmltodict
            # vs. the custom fetching mark was doing
            ref_images = self.source_metadata.get('originalRecord', {}).get(
                'reference-image', [])
            if type(ref_images) == dict:
                ref_images = [ref_images]
            for obj in ref_images:
                if max(int(obj.get('X')), int(obj.get('Y'))) > dim:
                    dim = max(int(obj.get('X')), int(obj.get('Y')))
                    best_image = obj.get('src')
            if best_image and not best_image.startswith('http'):
                best_image = f"http://content.cdlib.org/{best_image}"
        return best_image

    def map_item_count(self):
        '''Use reference-image-count value to determine compound objects.
        NOTE: value is not always accurate so only determines complex (-1)
        or not complex (no item_count value)
        '''
        item_count = None
        image_count = 0
        if 'originalRecord' in self.source_metadata:  # guard weird input
            ref_image_count = self.source_metadata.get(
                'originalRecord', {}).get('reference-image-count')
            if ref_image_count:
                image_count = ref_image_count[0]['text']
            if image_count > "1":
                item_count = "-1"
        return item_count

    def map_spatial(self):
        coverage = []
        if 'originalRecord' in self.source_metadata:  # guard weird input
            if 'coverage' in self.source_metadata.get('originalRecord'):
                coverage_data = iterify(
                    getprop(
                        self.source_metadata.get('originalRecord'),
                        "coverage"
                    ))
                # remove arks from data
                # and move the "text" value to
                for c in coverage_data:
                    if (not isinstance(c, str) and
                            not c.get('text').startswith('ark:')):
                        if ('q' in c.get('attrib', {}) and
                                'temporal' not in c.get(
                                    'attrib', {}).get('q')):
                            coverage.append(c.get('text'))
                        # collection 25496 has coverage values like
                        # A0800 & A1000 - drop these
                        anum_re = re.compile('A\d\d\d\d')
                        if ('q' not in c.get('attrib', {}) and
                                c.get('attrib', {}) is not None and
                                not anum_re.match(c.get('text'))):
                            coverage.append(c.get('text'))
        return coverage

    def map_temporal(self):
        temporal = []
        if 'originalRecord' in self.source_metadata:  # guard weird input
            if 'coverage' in self.source_metadata.get('originalRecord'):
                time_data = iterify(getprop(
                    self.source_metadata.get('originalRecord'), "coverage"))
                for t in time_data:
                    if ('q' in t.get('attrib', {})
                            and 'temporal' in t.get('attrib', {}).get('q')):
                        temporal.append(t.get('text'))
        return temporal

    def map_subject(self):
        subject_values = self.get_vals(
            "subject", suppress_attribs={'q': 'series'})
        subject_objs = [{'name': s} for s in subject_values]
        return subject_objs

    def to_dict(self):
        self.pre_mapped_data.update(self.mapped_data)
        return self.pre_mapped_data


class OacVernacular(Vernacular):
    record_cls = OacRecord

    # Directly copied from harvester codebase; not sure if this belongs here
    def _get_doc_ark(self, docHit):
        '''Return the object's ark from the xml etree docHit'''
        ids = docHit.find('meta').findall('identifier')
        ark = None
        for i in ids:
            if i.attrib.get('q', None) != 'local':
                try:
                    split = i.text.split('ark:')
                except AttributeError:
                    continue
                if len(split) > 1:
                    ark = ''.join(('ark:', split[1]))
        return ark

    # Directly copied from harvester codebase; not sure if this belongs here
    def parse_reference_image(self, tag):
        try:
            x = int(tag.attrib['X'])
        except ValueError:
            x = 0
        try:
            y = int(tag.attrib['Y'])
        except ValueError:
            y = 0
        src = f"http://content.cdlib.org/{tag.attrib['src']}"
        src = src.replace('//', '/').replace('/', '//', 1)
        data = {
            'X': x,
            'Y': y,
            'src': src,
        }
        return data

    # Directly copied from harvester codebase; not sure if this belongs here
    def parse_thumbnail(self, tag, document):
        ark = self._get_doc_ark(document)
        try:
            x = int(tag.attrib['X'])
        except ValueError:
            x = 0
        try:
            y = int(tag.attrib['Y'])
        except ValueError:
            y = 0
        src = f"http://content.cdlib.org/{ark}/thumbnail"
        src = src.replace('//', '/').replace('/', '//', 1)
        data = {
            'X': x,
            'Y': y,
            'src': src,
        }
        return data     # was not copied from harvester codebase - not sure?

    # Directly copied from harvester codebase
    def parse(self, api_response):
        crossQueryResult = ET.fromstring(api_response)
        facet_type_tab = crossQueryResult.find('facet')
        docHits = facet_type_tab.findall('./group/docHit')

        records = []
        for document in docHits:
            obj = defaultdict(list)
            meta = document.find('meta')
            for tag in meta:
                if tag.tag == 'google_analytics_tracking_code':
                    continue
                data = ''
                if tag.tag == 'reference-image':
                    obj[tag.tag].append(self.parse_reference_image(tag))
                elif tag.tag == 'thumbnail':
                    obj[tag.tag] = self.parse_thumbnail(tag, document)
                elif len(list(tag)) > 0:
                    # <snippet> tag breaks up text for findaid <relation>
                    for innertext in tag.itertext():
                        data = ''.join((data, innertext.strip()))
                    if data:
                        obj[tag.tag].append({
                            'attrib': tag.attrib,
                            'text': data
                        })
                else:
                    if tag.text:  # don't add blank ones
                        obj[tag.tag].append({
                            'attrib': tag.attrib,
                            'text': tag.text
                        })
            records.append(obj)

        return self.get_records(records)
