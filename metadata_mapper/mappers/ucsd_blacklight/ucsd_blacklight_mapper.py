import json
import re
from ..mapper import Record, Vernacular


class UcsdBlacklightMapper(Record):
    BASE_URL = "https://library.ucsd.edu/dc/object/"

    def UCLDC_map(self) -> dict:
        return {
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "contributor": self.map_contributor,
            "spacial": self.source_metadata.get("geographic_tesim"),
            "stateLocatedIn": {"name": "California"},
            "extent": self.source_metadata.get("extent_json_tesim"),
            "publisher": self.source_metadata.get("publisher_json_tesim"),
            "contributor": self.source_metadata.get("contributor"),
            "coverage": self.source_metadata.get("geographic_tesim"),
            "creator": self.map_creator,
            "date": self.map_date,
            "description": self.map_description,
            "format": self.map_format,
            "identifier": [],
            "is_part_of": self.map_is_part_of,
            "language": self.map_language,
            "rights": self.map_rights,
            "rightsHolder": self.source_metadata.get("rightsHolder_tesim"),
            "spatial": self.source_metadata.get("geographic_tesim"),
            "spec_type": self.map_spec_type,
            "subject": self.map_subject,
            "temporal": self.map_temporal,
            "title": self.map_title,
            "alternative_title": self.map_alternative_title,
            "type": self.map_type,
            "genre": self.map_genre,
            "provenance": "",
            "source": "",
            "relation": self.map_relation
        }

    def map_is_shown_at(self) -> str:
        id_t = self.source_metadata.get('id_t')
        return f"{self.BASE_URL}{id_t}"

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

        id_t = self.source_metadata.get("id_t")
        return f"{self.BASE_URL}{id_t}/_{file_id}"

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
        return [c for r, c in self.relationship.items()
                if r in contributor_role_list]

    def map_creator(self) -> [None, list]:
        if not self.relationship:
            return

        if len(self.relationship) == 1:
            return list(self.relationship.values())[0]
        else:
            return [c for r, c in self.relationship.items() if r in
                    creator_role_list]

    def map_relation(self) -> [None, str]:
        related_resource = self.source_metadata. \
            get("related_resource_json_tesim", [])
        for relation in related_resource:
            if relation.get("type") == "online finding aid":
                return [relation.get("uri")]

    def map_date(self) -> [None, dict]:
        # If the dates don't look right, skip the item, otherwise we end up
        # with an error in map_couch_to_solr_doc()
        date_regex = r"^\d{4}(-\d{2}-\d{2})?$"
        dates = [date for date in self.source_metadata.get("date_json_tesim", [])
                 if re.match(date_regex, date.get("endDate")) and
                 re.match(date_regex, date.get("beginDate"))]

        if not dates:
            return

        for date in filter(None, dates):
            if date.get("type") == "creation":
                break
        else:  # no creation date, use first date
            date = dates[0]

        return {
            "end": date.get("endDate"),
            "begin": date.get("beginDate"),
            "displayDate": date.get("value")
        }

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

    def map_identifier(self):
        pass

    def map_language(self) -> list:
        values = self.source_metadata.get('language_tesim', [])
        for lang in self.source_metadata.get("sourceResource", {}). \
                get("language", []):
            lang["iso639"] = lang.get("code")
            del lang["code"]
            del lang["externalAuthority"]
            values.append(lang)

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

    # Leftover from legacy mapper
    def map_has_view(self):
        pass

    # Leftover from legacy mapper
    def map_object(self):
        pass

    # Leftover from legacy mapper
    def map_temporal(self):
        pass

    # Leftover from legacy mapper
    def map_spec_type(self):
        pass

    # Leftover from legacy mapper
    def map_is_part_of(self):
        pass


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


class UcsdBlacklightVernacular(Vernacular):
    record_cls = UcsdBlacklightMapper

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
