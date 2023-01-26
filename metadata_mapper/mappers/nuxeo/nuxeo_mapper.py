import json
from ..mapper import Vernacular, Record


class NuxeoRecord(Record):
    def to_UCLDC(self):
        source_metadata = self.source_metadata.get('properties')

        mapped_data = {
            "calisphere-id": self.source_metadata.get("uid"),
            "isShownAt": (
                f"https://calisphere.org/item/"
                f"{self.source_metadata.get('uid', '')}"
            ),
            "source": source_metadata.get("ucldc_schema:source"),
            'location': [source_metadata.get(
                'ucldc_schema:physlocation', None)],
            'rightsHolder': (
                self.collate_subfield('ucldc_schema:rightsholder', 'name') +
                [source_metadata.get('ucldc_schema:rightscontact')]
            ),
            'rightsNote': (
                (source_metadata.get('ucldc_schema:rightsnotice', []) or []) +
                [source_metadata.get('ucldc_schema:rightsnote', '')]
            ),
            'dateCopyrighted': source_metadata.get(
                'ucldc_schema:rightsstartdate', None),
            'transcription': source_metadata.get(
                'ucldc_schema:transcription', None),
            'contributor': self.collate_subfield(
                'ucldc_schema:contributor', 'name'),
            'creator': self.collate_subfield('ucldc_schema:creator', 'name'),
            'date': self.collate_subfield('ucldc_schema:date', 'date'),
            'description': self.map_description(),
            'extent': [source_metadata.get('ucldc_schema:extent', None)],
            'format': [source_metadata.get('ucldc_schema:physdesc', None)],
            'identifier': (
                [source_metadata.get('ucldc_schema:identifier')] +
                source_metadata.get('ucldc_schema:localidentifier', [])
            ),
            'id': (
                [source_metadata.get('ucldc_schema:identifier')] +
                source_metadata.get('ucldc_schema:localidentifier', [])
            ),
            'language': self.map_language(),
            'publisher': list(
                source_metadata.get('ucldc_schema:publisher', [])),
            'relation': list(
                source_metadata.get('ucldc_schema:relatedresource', [])),
            'rights': self.map_rights(),
            'spatial': self.map_spatial(),
            'subject': (
                    self.collate_subfield(
                        'ucldc_schema:subjecttopic', 'heading') +
                    self.collate_subfield('ucldc_schema:subjectname', 'name')
            ),
            'temporalCoverage': list(
                source_metadata.get('ucldc_schema:temporalcoverage', [])),
            'title': [source_metadata.get('dc:title')],
            'type': [source_metadata.get('ucldc_schema:type', None)],
            'provenance': source_metadata.get('ucldc_schema:provenance', None),
            'alternativeTitle': list(
                source_metadata.get('ucldc_schema:alternativetitle', [])),
            'genre': self.collate_subfield('ucldc_schema:formgenre', 'heading')
        }

        self.mapped_data = mapped_data
        return self

    def to_dict(self):
        return self.mapped_data

    description_type_labels = {
        'scopecontent': 'Scope/Content',
        'acquisition': 'Acquisition',
        'bibliography': 'Bibliography',
        'bioghist': 'Biography/History',
        'biography': 'Biography/History',
        'biographical': 'Biography/History',
        'citereference': 'Citation/Reference',
        'conservation': 'Conservation History',
        'creationprod': 'Creation/Production Credits',
        'date': 'Date Note',
        'exhibitions': 'Exhibitions',
        'funding': 'Funding',
        'marks': 'Annotations/Markings',
        'language': 'Language',
        'performers': 'Performers',
        'prefercite': 'Preferred Citation',
        'prodcredits': 'Production Credits',
        'venue': 'Venue',
        'condition': 'Condition',
        'medium': 'Medium',
        'technique': 'Technique'
    }

    def map_description(self):
        desc_data = []
        raw_data = self.source_metadata.get('ucldc_schema:description', [])
        if isinstance(raw_data, list):
            desc_data = [self.unpack_description_data(d) for d in raw_data]
        else:
            desc_data = [self.unpack_description_data(raw_data)]
        return desc_data

    def unpack_description_data(self, data):
        '''See if dict or basestring and unpack value'''
        unpacked = None
        if isinstance(data, dict):
            # make robust to not break
            data_type = data.get('type', '').strip()
            # print(f"Data CODE:{ data_type }")
            if self.description_type_labels.get(data_type, ''):
                data_type = self.description_type_labels.get(data_type, '')
                # print(f"Data Readable:{ data_type }")
            item = data.get('item', '')
            if item:
                unpacked = u'{}: {}'.format(data_type, item)
            else:
                unpacked = ''
        else:
            unpacked = data
        return unpacked

    def map_language(self):
        languages = []
        for lang in self.source_metadata.get('ucldc_schema:language', []):
            if lang['language']:
                languages.append(lang['language'])
            if lang['languagecode']:
                languages.append(lang['languagecode'])
        return languages
        # return [{'iso639_3': l} for l in languages]

    def map_rights_codes(self, rights_str):
        '''Map the "coded" values of the rights status to a nice one for
        display
        '''
        decoded = rights_str
        if rights_str == 'copyrighted':
            decoded = 'Copyrighted'
        elif rights_str == 'publicdomain':
            decoded = 'Public Domain'
        elif rights_str == 'unknown':
            decoded = 'Copyright Unknown'
        return decoded

    def map_rights(self):
        rights_status = self.source_metadata.get('ucldc_schema:rightsstatus')
        rights_status = [self.map_rights_codes(rights_status)]
        rights_statement = [self.source_metadata.get(
            'ucldc_schema:rightsstatement')]
        return rights_status + rights_statement

    def map_spatial(self):
        spatial = []
        for place in self.source_metadata.get('ucldc_schema:place', []):
            if place['name']:
                spatial.append(place['name'])
            if place['coordinates']:
                spatial.append(place['coordinates'])
        return [{'text': s} for s in spatial]

    def map_is_shown_at(self):
        return super().map_is_shown_at()
    
    def map_is_shown_by(self):
        return super().map_is_shown_by()

class NuxeoVernacular(Vernacular):
    record_cls = NuxeoRecord

    def parse(self, api_response):
        records = json.loads(api_response)['entries']
        return [self.record_cls(self.collection_id, record)
                for record in records]
