import json
from mapper import Mapper

class NuxeoMapper(Mapper):    
    def get_records(self, vernacular_page):
        records = json.loads(vernacular_page)['entries']
        return records

    def map_record(self, record):
        source_metadata = record.get('properties')

        def collate_subfield(field, subfield):
            return [f[subfield] for f in source_metadata.get(field, [])]

        mapped_data = {
            "isShownAt": f"https://calisphere.org/item/{source_metadata.get('uid', '')}",
            "source": source_metadata.get("ucldc_schema:source"),
            'location': source_metadata.get('ucldc_schema:physlocation', None),
            'rightsHolder': (
                collate_subfield('ucldc_schema:rightsholder', 'name') + 
                [source_metadata.get('ucldc_schema:rightscontact')]
            ),
            'rightsNote': (
                (source_metadata.get('ucldc_schema:rightsnotice', []) or []) +
                [source_metadata.get('ucldc_schema:rightsnote', '')]
            ),
            'dateCopyrighted': source_metadata.get('ucldc_schema:rightsstartdate', None),
            'transcription': source_metadata.get('ucldc_schema:transcription', None),
            'contributor': collate_subfield(
                'ucldc_schema:contributor', 'name'),
            'creator': collate_subfield('ucldc_schema:creator', 'name'),
            'date': collate_subfield('ucldc_schema:date', 'date'),
            'description': self.map_description(source_metadata),
            'extent': source_metadata.get('ucldc_schema:extent', None),
            'format': source_metadata.get('ucldc_schema:physdesc', None),
            'identifier': (
                [source_metadata.get('ucldc_schema:identifier')] +
                source_metadata.get('ucldc_schema:localidentifier', [])
            ),
            'language': self.map_language(source_metadata),
            'publisher': list(
                source_metadata.get('ucldc_schema:publisher', [])),
            'relation': list(
                source_metadata.get('ucldc_schema:relatedresource', [])),
            'rights': self.map_rights(source_metadata),
            'spatial': self.map_spatial(source_metadata),
            'subject': (
                    collate_subfield('ucldc_schema:subjecttopic', 'heading') +
                    collate_subfield('ucldc_schema:subjectname', 'name')
            ),
            'temporalCoverage': list(
                source_metadata.get('ucldc_schema:temporalcoverage', [])),
            'title': [source_metadata.get('dc:title')],
            'type': source_metadata.get('ucldc_schema:type', None),
            'provenance': source_metadata.get('ucldc_schema:provenance', None),
            'alternativeTitle': list(
                source_metadata.get('ucldc_schema:alternativetitle', [])),
            'genre': collate_subfield('ucldc_schema:formgenre', 'heading'),
        }

        return mapped_data

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

    def map_description(self, source_metadata):
        desc_data = []
        raw_data = source_metadata.get('ucldc_schema:description', [])
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

    def map_language(self, source_metadata):
        languages = []
        for lang in source_metadata.get('ucldc_schema:language', []):
            if lang['language']:
                languages.append(lang['language'])
            if lang['languagecode']:
                languages.append(lang['languagecode'])
        return [{'iso639_3': l} for l in languages]

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

    def map_rights(self, source_metadata):
        rights_status = source_metadata.get('ucldc_schema:rightsstatus')
        rights_status = [self.map_rights_codes(rights_status)]
        rights_statement = [source_metadata.get('ucldc_schema:rightsstatement')]
        return rights_status + rights_statement

    def map_spatial(self, source_metadata):
        spatial = []
        for place in source_metadata.get('ucldc_schema:place', []):
            if place['name']:
                spatial.append(place['name'])
            if place['coordinates']:
                spatial.append(place['coordinates'])
        return [{'text': s} for s in spatial]
