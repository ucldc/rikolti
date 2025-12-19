from ..islandora_mapper import IslandoraRecord, IslandoraVernacular


class GlbthsRecord(IslandoraRecord):
    def UCLDC_map(self):
        return {
            "provenance": self.map_provenance(),
            "type": self.map_type(),
            "extent": self.source_metadata.get('extent', None),
            "genre": self.source_metadata.get('genre', None),
            "description": self.map_description(),
            "contributor": self.scrub_unknowns('contributor'),
            "creator": self.scrub_unknowns('creator'),
            "format": self.map_format(),
            "date": self.map_date(),
            "rights": self.source_metadata.get("rights", None),
            "relation": self.collate_fields([
                "conformsTo",
                "hasFormat",
                "hasPart",
                "hasVersion",
                "isFormatOf",
                # "isPartOf", included in oai_mapper, removed here
                "isReferencedBy",
                "isReplacedBy",
                "isRequiredBy",
                "isVersionOf",
                "references",
                "relation",
                "replaces",
                "require"
            ]),
            "identifier": self.map_identifier(),
            "format": self.map_format()
        }

    def map_provenance(self):
        provenance = self.source_metadata.get('provenance', None)
        if provenance:
            provenance.append(
                "California Revealed is supported by the U.S. Institute of "
                "Museum and Library Services under the provisions of the "
                "Library Services and Technology Act, administered in "
                "California by the State Librarian."
            )
        return provenance

    def map_type(self):
        type_value = self.source_metadata.get('type', [None])
        if type_value[0] == 'Item':
            return self.source_metadata.get('medium', None)
        else:
            return type_value
    
    def map_description(self):
        descriptions = self.source_metadata.get('description', [])
        #scrub CAVPP and California Revealed from description
        descriptions = [
            d for d in descriptions 
            if 'California Audiovisual Preservation Project (CAVPP)' not in d 
            and 'California Revealed' not in d
        ]
        return descriptions

    def scrub_unknowns(self, field_name: str):
        values = self.source_metadata.get(field_name, [])
        if isinstance(values, str):
            values = [values]
        values = [v for v in values if 'unknown' not in v.lower()]
        return values if values else None

    def map_format(self):
        formats = self.scrub_unknowns('format') or []
        mediums = self.scrub_unknowns('medium') or []
        all_formats = formats + mediums
        return all_formats if all_formats else None

    def map_date(self):
        created = self.scrub_unknowns('created') or []
        issued = self.scrub_unknowns('issued') or []
        date = self.scrub_unknowns('date') or []
        combined_dates = created + issued + date
        return combined_dates if combined_dates else None

    def map_identifier(self):
        #scrub archive.org and cavpp values from identifier
        values = []
        fields = ['bibliographicCitation', 'identifier', 'identifier.ark']
        for field in fields:
            field_values = self.source_metadata.get(field, [])
            if isinstance(field_values, str):
                field_values = [field_values]
            values.extend(field_values)

        values = [v for v in values if 'archive.org' not in v and 'cavpp' not in v]
        return values

    def map_is_shown_at(self):
        thumbnails = self.source_metadata.get('identifier.thumbnail', [])
        if isinstance(thumbnails, str):
            thumbnails = [thumbnails]
        datastream_thumbnails = [t for t in thumbnails if '/datastream/TN' in t]

        identifiers = self.source_metadata.get('identifier', [])
        if isinstance(identifiers, str):
            identifiers = [identifiers]
        node_identifiers = [i for i in identifiers if 'node' in i]

        if datastream_thumbnails:
            return datastream_thumbnails[0].split('/datastream/TN')[0]
        elif node_identifiers:
            return node_identifiers[0]
        else:
            return None
    
    def map_is_shown_by(self):
        return self.source_metadata.get('identifier.thumbnail', [None])[0]


class GlbthsVernacular(IslandoraVernacular):
    record_cls = GlbthsRecord
