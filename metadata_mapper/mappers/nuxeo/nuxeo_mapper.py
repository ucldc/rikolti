import json

from ..mapper import Record, Vernacular


class NuxeoRecord(Record):
    def to_UCLDC(self):
        self.original_metadata = self.source_metadata
        self.source_metadata = self.source_metadata.get('properties')
        return super().to_UCLDC()

    def UCLDC_map(self):
        return {
            "calisphere-id": self.original_metadata.get("uid"),
            "isShownAt": (
                f"https://calisphere.org/item/"
                f"{self.original_metadata.get('uid', '')}"
            ),
            "isShownBy": self.map_thumbnail_source(),
            "media_source": self.map_media_source(),
            "source": [self.source_metadata.get("ucldc_schema:source")],
            'location': [self.source_metadata.get(
                'ucldc_schema:physlocation', None)],
            'rightsHolder': (
                self.collate_subfield('ucldc_schema:rightsholder', 'name')() +
                [self.source_metadata.get('ucldc_schema:rightscontact')]
            ),
            'rightsNote': [
                self.source_metadata.get('ucldc_schema:rightsnotice'),
                self.source_metadata.get('ucldc_schema:rightsnote')
            ],
            'dateCopyrighted': self.source_metadata.get(
                'ucldc_schema:rightsstartdate', None),
            'transcription': self.source_metadata.get(
                'ucldc_schema:transcription', None),
            'contributor': self.collate_subfield(
                'ucldc_schema:contributor', 'name'),
            'creator': self.collate_subfield('ucldc_schema:creator', 'name'),
            'date': self.collate_subfield('ucldc_schema:date', 'date'),
            'description': self.map_description(),
            'extent': [self.source_metadata.get('ucldc_schema:extent', None)],
            'format': [self.source_metadata.get('ucldc_schema:physdesc', None)],
            'identifier': (
                [self.source_metadata.get('ucldc_schema:identifier')] +
                self.source_metadata.get('ucldc_schema:localidentifier', [])
            ),
            'id': (
                [self.source_metadata.get('ucldc_schema:identifier')] +
                self.source_metadata.get('ucldc_schema:localidentifier', [])
            ),
            'language': self.map_language(),
            'publisher': list(
                self.source_metadata.get('ucldc_schema:publisher', [])),
            'relation': list(
                self.source_metadata.get('ucldc_schema:relatedresource', [])),
            'rights': self.map_rights(),
            'spatial': self.map_spatial(),
            'subject': (
                    self.collate_subfield(
                        'ucldc_schema:subjecttopic', 'heading')() +
                    self.collate_subfield('ucldc_schema:subjectname', 'name')()
            ),
            'temporalCoverage': list(
                self.source_metadata.get('ucldc_schema:temporalcoverage', [])),
            'title': [self.source_metadata.get('dc:title')],
            'type': [self.source_metadata.get('ucldc_schema:type', None)],
            'provenance': self.source_metadata.get('ucldc_schema:provenance', None),
            'alternativeTitle': list(
                self.source_metadata.get('ucldc_schema:alternativetitle', [])),
            'genre': self.collate_subfield('ucldc_schema:formgenre', 'heading')()
        }

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
        rights_status = self.source_metadata.get(
            'ucldc_schema:rightsstatus')
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

    def map_media_source(self):
        source_type = self.original_metadata.get('type')
        valid_types = [
            'CustomFile',
            'Organization',  # (this is actually a file)
            'CustomAudio',
            'CustomVideo',
            'SampleCustomPicture'
        ]
        # we don't yet know how to handle other types
        if source_type not in valid_types:
            return None

        # get the file content
        file_content = self.source_metadata.get('file:content')
        if file_content and file_content.get('name') == 'empty_picture.png':
            file_content = None
        elif file_content and not file_content.get('name'):
            file_content['name'] = self.source_metadata.get('file:filename')

        # for Video, overwrite file_content with nuxeo transcoded video file
        # mp4 url in properties.vid:transcodedVideos, if it exists
        if source_type == 'CustomVideo':
            transcoded_videos = self.source_metadata.get('vid:transcodedVideos', [])
            for tv in transcoded_videos:
                if tv['content']['mime-type'] == 'video/mp4':
                    file_content = tv['content']
                    break

        # this is the mapping part where we map to our data model
        media_source = None
        if file_content:
            media_source = {
                'url': file_content.get('data', '').strip(),
                'mimetype': file_content.get('mime-type', '').strip(),
                'filename': file_content.get('name', '').strip(),
                'nuxeo_type': source_type
            }

        return media_source

    def map_thumbnail_source(self):
        source_type = self.original_metadata.get('type')
        valid_types = [
            'CustomVideo',
            'CustomFile',
            'Organization',  # (this is actually a file)
            'SampleCustomPicture'
        ]
        # we don't know how to make thumbnails for other types
        if source_type not in valid_types:
            return None

        # thumbnail source is the same as media source, except
        # in the case of images!
        thumbnail_source = self.map_media_source()

        # if it's a SampleCustomPicture, overwrite thumbnail location
        # URL with Nuxeo thumbnail url (see legacy logic for why this
        # comes after thumbnail_source = self.map_media_source())
        if (source_type == 'SampleCustomPicture'):
            # Rikolti Logic:
            picture_views = self.source_metadata.get("picture:views")
            medium_view = list(
                filter(lambda x: x['title'] == 'Medium', picture_views)
            )
            if medium_view:
                medium_view = medium_view[0].get('content')
                thumbnail_source = {
                    'url': medium_view.get('data', '').strip(),
                    'mimetype': medium_view.get('mime-type', '').strip(),
                    'filename': medium_view.get('name', '').strip(),
                    'nuxeo_type': source_type
                }
            else:
                print(json.dumps(picture_views))
            # Legacy Logic:
            # uid = self.original_metadata.get('uid', '')
            # thumbnail_source['url'] = (
            #     f"https://nuxeo.cdlib.org/nuxeo/nxpicsfile/default/"
            #     f"{uid}/Medium:content/"
            # )
            # thumbnail_source['mimetype'] = 'image/jpeg'
            # thumbnail_source['filename'] = (
            #     thumbnail_source['filename'].split('.')[0] + '.jpg')

        return thumbnail_source


class NuxeoVernacular(Vernacular):
    record_cls = NuxeoRecord

    def parse(self, api_response):
        return self.get_records(json.loads(api_response)['entries'])
