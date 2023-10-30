import json
from ..mapper import Record, Validator, Vernacular
from typing import Any


class UcsdBlacklightMapper(Record):
    BASE_URL = "https://library.ucsd.edu/dc/object/"

    BASE_ARK = "ark:/20775/"

    def UCLDC_map(self) -> dict:
        return {
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "contributor": self.map_contributor,
            "stateLocatedIn": {"name": "California"},
            "extent": self.source_metadata.get("extent_json_tesim"),
            "publisher": self.source_metadata.get("publisher_json_tesim"),
            "coverage": self.source_metadata.get("geographic_tesim"),
            "creator": self.map_creator,
            "date": self.map_date,
            "description": self.map_description,
            "identifier": self.map_identifier,
            "format": self.map_format,
            "language": self.map_language,
            "rights": self.map_rights,
            "rightsHolder": self.source_metadata.get("rightsHolder_tesim"),
            "spatial": self.source_metadata.get("geographic_tesim"),
            "subject": self.map_subject,
            "title": self.map_title,
            "alternative_title": self.map_alternative_title,
            "type": self.map_type,
            "genre": self.map_genre,
            "provenance": "",
            "source": "",
            "relation": self.map_relation
        }

    def map_is_shown_at(self) -> str:
        id = self.source_metadata.get('id')
        return f"{self.BASE_URL}{id}"

    def map_is_shown_by(self) -> [None, str]:
        """
        TODO: handle complex objects (this todo from legacy mapper)
        """
        matches = ["image-preview", "image-service"]
        break_matches = ["image-service"]

        file_id = None
        for file in self.source_metadata.get("files_tesim", []):
            use = file.get("use")
            if use in matches:
                file_id = file.get("id")
            if use in break_matches:
                break
        else:
            for file in self.source_metadata.get("component_1_files_tesim", []):
                use = file.get("use")
                if use in matches:
                    file_id = "1_" + file.get("id")
                if use in break_matches:
                    break

            # TODO: this is temp fix, but will help (this todo from legacy mapper)
            if not file_id:
                file_id = "1_3.jpg"

        if not file_id:
            return None

        id = self.source_metadata.get("id")
        return f"{self.BASE_URL}{id}/_{file_id}"

    @property
    def relationship(self) -> str:
        return self.source_metadata.get("relationship_json_tesim")[0]

    def map_description(self) -> list:
        descriptions = []
        for note_type in otherNote_types:
            note_list = self.extract_notes_by_type(note_type)
            for note in note_list:
                if "value" in note:
                    descriptions.append(note.get("value"))

        # Taken directly from legacy mapper
        for sc in filter(None,
                         self.source_metadata.get('scopeContentNote_json_tesim', [])):
            value = None
            try:
                value = sc['value']
            except TypeError:
                try:
                    j = json.loads(sc)
                    value = j.get('value')
                except (ValueError, TypeError):
                    pass
            if value:
                descriptions.append(value)

        return descriptions

    def map_contributor(self) -> list:
        """
        Match contributor role names case insensitively
        """
        lower_contributor_role_list = [r.lower() for r in contributor_role_list]
        return [item for r, c in self.relationship.items()
                if r.lower() in lower_contributor_role_list for item in c]

    def map_creator(self) -> [None, list]:
        """
        Does this need to be case-insensitive like contributors? Seems like it does not.
        """
        if not self.relationship:
            return

        if len(self.relationship) == 1:
            return list(self.relationship.values())[0]

        creators = []
        for r, c in self.relationship.items():
            if r in creator_role_list:
                creators.extend(c)
        return creators

    def map_relation(self) -> [None, str]:
        related_resource = self.source_metadata. \
            get("related_resource_json_tesim", [])
        for relation in related_resource:
            if relation.get("type") == "online finding aid":
                return [relation.get("uri")]

    def map_date(self) -> [None, dict]:
        date_list = self.source_metadata.get('date_json_tesim', [])
        if not len(date_list):
            return

        # "creation" date is priority, otherwise use first date
        for date_obj in filter(None, date_list):
            if date_obj.get('type') == 'creation':
                break
        else:
            date_obj = date_list[0]

        return [{
            "end": date_obj.get('endDate'),
            "begin": date_obj.get('beginDate'),
            "displayDate": date_obj.get('value')
        }]

    def extract_notes_by_type(self, note_type) -> list:
        value = self.source_metadata.get('otherNote_json_tesim')
        if not value:
            return []

        return [v for v in filter(None, value) if v.get("type") == note_type]

    def map_format(self) -> list:
        values = self.extract_notes_by_type('general physical description')
        values.extend(self.extract_notes_by_type('physical description'))
        return [v.get("value") if isinstance(v, dict) else v for v in values]

    def map_genre(self) -> [None, dict]:
        genre = self.source_metadata.get('genreForm_tesim', None)
        return {"genre": genre} if genre else None

    def map_identifier(self) -> list:
        identifier = self.source_metadata.get('id')
        if isinstance(identifier, list):
            identifier = identifier[0]

        return [f"{self.BASE_ARK}{identifier}"]

    def map_language(self) -> list:
        values = self.source_metadata.get('language_tesim', [])
        for language_data in self.source_metadata.get("language_json_tesim", []):
            values.append(language_data.get("code"))

        return list(filter(None, values))

    def map_rights(self) -> list:
        return [obj.get(tag)
                for obj in self.source_metadata.get("copyright_tesim", [])
                for tag in ["status", "note", "purposeNote"] if obj.get(tag)]

    def map_subject(self) -> list:
        # Typecast as a string to avoid stray integers, which were encountered
        # in collection 26426
        return list(filter(None, [{"name": str(value)} for field in
                                  subject_source_fields for value in
                                  self.source_metadata.get(field, [])
                                  if value]))

    def map_title(self) -> list:
        value = self.source_metadata.get('title_json_tesim')
        if not value:
            return []

        title = value[0].get('name')
        return [title]

    def map_alternative_title(self) -> list:
        tags = ["variant", "abbreviationVariant", "acronymVariant", "expansionVariant"]

        title = self.source_metadata.get('title_json_tesim', None)
        if not title:
            return []

        return list(filter(None, [title[0].get(tag) for tag in tags]))

    def map_type(self) -> str:
        resource_type = self.source_metadata.get("resource_type_tesim")
        if isinstance(resource_type, dict) and resource_type.has("type"):
            resource_type = resource_type["type"][0]
        return resource_type


subject_source_fields = ["subject_tesim", "topic_tesim", "subject_topic_tesim",
                         "complexSubject_tesim", "anatomy_tesim",
                         "commonName_tesim", "conferenceName_tesim",
                         "corporateName_tesim", "culturalContext_tesim",
                         "cruise_tesim", "familyName_tesim", "genreForm_tesim",
                         "geographic_tesim", "lithology_tesim",
                         "occupation_tesim", "personalName_tesim",
                         "scientificName_tesim", "series_tesim",
                         "temporal_tesim"]

otherNote_types = ["arrangement", "bibliography", "biography", "classification",
                   "credits", "custodial history", "description",
                   "digital origin", "edition", "funding", "inscription",
                   "local attribution", "location of originals",
                   "material details", "note", "performers",
                   "preferred citation", "publication", "related publications",
                   "scope and content", "series", "site",
                   "statement of responsibility", "table of contents",
                   "technical requirements", "thesis", "venue"]

creator_role_list = ["Creator", "Artist", "Author", "Composer", "Creator",
                     "Filmmaker", "Photographer", "Principal investigator"]

contributor_role_list = ["Contributor", "Abridger", "Actor", "Adapter",
                         "Addressee", "Analyst", "Animator", "Annotator",
                         "Applicant", "Architect", "Arranger", "Art copyist",
                         "Art director", "Artistic director", "Assignee",
                         "Associated name", "Attributed name", "Auctioneer",
                         "Author in quotations or text abstracts",
                         "Author of afterword, colophon, etc",
                         "Author of dialog", "Author of introduction, etc",
                         "Autographer", "Bibliographic antecedent", "Binder",
                         "Binding designer", "Blurb writer", "Book designer",
                         "Book producer", "Bookjacket designer",
                         "Bookplate designer", "Bookseller", "Braille embosser",
                         "Broadcaster", "Calligrapher", "Cartographer",
                         "Caster", "Censor", "Choreographer", "Cinematographer",
                         "Client", "Collection registrar", "Collector",
                         "Collotyper", "Colorist", "Commentator",
                         "Commentator for written text", "Compiler",
                         "Complainant", "Complainant-appellant", "Compositor",
                         "Conceptor", "Conductor", "Conservator", "Consultant",
                         "Consultant to a project", "Contestant",
                         "Contestant-appellant", "Contestant-appellee",
                         "Contestee", "Contestee-appellant",
                         "Contestee-appellee", "Contractor", "Contributor",
                         "Co-principal investigator", "Copyright claimant",
                         "Copyright holder", "Corrector", "Correspondent",
                         "Costume designer", "Court governed", "Court reporter",
                         "Cover designer", "Cruise", "Curator", "Dancer",
                         "Data contributor", "Data manager", "Dedicatee",
                         "Dedicator", "Degree granting institution",
                         "Degree supervisor", "Delineator", "Depicted",
                         "Depositor", "Designer", "Director", "Dissertant",
                         "Distribution place", "Distributor", "Donor",
                         "Draftsman", "Dubious author", "Editor",
                         "Editor of compilation", "Editor of moving image work",
                         "Electrician", "Electrotyper", "Enacting jurisdiction",
                         "Engineer", "Engraver", "Etcher", "Event place",
                         "Expert", "Facsimilist", "Field assistant",
                         "Field director", "Film director", "Film distributor",
                         "Film editor", "Film producer", "First party",
                         "Forger", "Former owner", "Funder",
                         "Geographic information specialist", "Honoree", "Host",
                         "Host institution", "Illuminator", "Illustrator",
                         "Inscriber", "Instrumentalist", "Interviewee",
                         "Interviewer", "Inventor", "Issuing body",
                         "Jurisdiction governed", "Laboratory",
                         "Laboratory assistant", "Laboratory director",
                         "Landscape architect", "Lead", "Lender", "Libelant",
                         "Libelant-appellant", "Libelant-appellee", "Libelee",
                         "Libelee-appellant", "Libelee-appellee", "Librettist",
                         "Licensee", "Licensor", "Lighting designer",
                         "Lithographer", "Lyricist", "Manufacture place",
                         "Manufacturer", "Marbler", "Markup editor", "Medium",
                         "Metadata contact", "Metal-engraver", "Minute taker",
                         "Moderator", "Monitor", "Music copyist",
                         "Musical director", "Musician", "Narrator",
                         "Onscreen presenter", "Opponent", "Organizer",
                         "Originator", "Other", "Owner", "Panelist",
                         "Papermaker", "Patent applicant", "Patent holder",
                         "Patron", "Performer", "Permitting agency",
                         "Plaintiff", "Plaintiff-appellant",
                         "Plaintiff-appellee", "Platemaker", "Praeses",
                         "Presenter", "Printer", "Printer of plates",
                         "Printmaker", "Process contact", "Producer",
                         "Production company", "Production designer",
                         "Production manager", "Production personnel",
                         "Production place", "Programmer", "Project director",
                         "Proofreader", "Provider", "Publication place",
                         "Publishing director", "Puppeteer", "Radio director",
                         "Radio producer", "Recording engineer", "Recordist",
                         "Redaktor", "Renderer", "Reporter", "Repository",
                         "Research team head", "Research team member",
                         "Researcher", "Respondent", "Responsible party",
                         "Restager", "Restorationist", "Reviewer", "Rubricator",
                         "Scenarist", "Scientific advisor", "Screenwriter",
                         "Scribe", "Sculptor", "Second party", "Secretary",
                         "Seller", "Set designer", "Setting", "Signer",
                         "Singer", "Sound designer", "Speaker", "Sponsor",
                         "Stage director", "Stage manager", "Standards body",
                         "Stereotyper", "Storyteller", "Supporting host",
                         "Surveyor", "Teacher", "Technical director",
                         "Television director", "Television producer",
                         "Thesis advisor", "Transcriber", "Translator",
                         "Type designer", "Typographer", "University place",
                         "Vessel", "Videographer", "Voice actor", "Witness",
                         "Wood engraver", "Woodcutter",
                         "Writer of accompanying material",
                         "Writer of added commentary", "Writer of added lyrics",
                         "Writer of added text", "Writer of introduction",
                         "Writer of preface",
                         "Writer of supplementary textual content"]


class UcsdBlacklightValidator(Validator):
    def setup(self, **options):
        self.add_validatable_field(
            field="identifier", type=Validator.list_of(str),
            validations=[
                UcsdBlacklightValidator.identifier_content_match
            ]
        )

    @staticmethod
    def identifier_content_match(validation_def: dict, rikolti_value: Any,
                                 comparison_value: Any) -> None:
        """
        The `identifier` field will be populated with the ARK going forward. We don't
        need validator errors when this happens.
        """
        if comparison_value is None and isinstance(rikolti_value, list) and \
                len(rikolti_value) == 1 and isinstance(rikolti_value[0], str) and \
                rikolti_value[0].startswith("ark:/"):
            return None
        return Validator.content_match(validation_def, rikolti_value, comparison_value)


class UcsdBlacklightVernacular(Vernacular):
    record_cls = UcsdBlacklightMapper
    validator = UcsdBlacklightValidator

    def parse(self, api_response) -> list:
        def modify_record(record) -> dict:
            record.update({"calisphere-id": f"{self.collection_id}--"
                                            f"{record.get('id')}"})
            return record

        page_element = json.loads(api_response)
        records = page_element.get("response", {}).get("docs", [])

        return self.get_records([modify_record(record) for record in records])

    def skip(self, record) -> bool:
        """
        Given a record, determines if it should be skipped due to the culturally
        sensitive content

        Parameters:
            record: dict
        """
        notes = [json.loads(note) for note
                 in record.get("otherNote_json_tesim", [])]

        matches = [note for note in notes
                   if note.get("type") == "note"]

        for note in matches:
            if isinstance(note, (str, bytes)):
                note = [note]

            if any([r and r.startswith("Culturally sensitive content:")
                    for r in note]):
                return True

        return False
