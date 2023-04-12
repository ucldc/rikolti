from .flickr_mapper import FlickrRecord, FlickrVernacular
import re


class SpplRecord(FlickrRecord):
    def UCLDC_map(self):
        return {
            "description": self.map_description,
            "identifier": self.map_identifier,
            "type": self.map_type,
            "rights": self.map_rights,
            "provenance": self.map_provenance
        }

    @property
    def source_description(self):
        return self.source_metadata.get("description", {}).get("_content")

    @staticmethod
    def get_mapping_configuration():
        """
        Provides configuration for locating and extracting metadata values from
        the description field using regex. The "key" field gives the regex a
        name. The capture group in the regex is the value that we extract for
        that field. The entire match (including the parts outside the capture
        group), are replaced with an empty string in the description field.
        The "keep" field indicates whether we want to store the value or not, it
        will be removed from the description string in all cases.

        The `rights_information` regex is a catch all that will capture
        everything after "Rights Information:" appears in the description,
        which is why it is last.
        """
        return [
            {
                "key": "type",
                "regex": r"Type:([^\n]+)\n\s*\n"
            },
            {
                "key": "provenance",
                "regex": r"Source:([^\n]+)\n\s*\n"
            },
            {
                "key": "date",
                "regex": r"Date:([^\n]+)\n\s*\n"
            },
            {
                "key": "identifier",
                "regex": r"Identifier:([^\n]+)\n\s*\n"
            },
            {
                "key": "owner",
                "regex": r"Owner:([^\n]+)\n\s*\n",
                "keep": False
            },
            {
                "key": "previous_identifier_discard",
                "regex": r"Previous Identifier: *N/A\n\s*\n",
                "keep": False
            },
            {
                "key": "previous_identifier",
                "regex": r"Previous Identifier:([^\n]+)\n\s*\n"
            },

            {
                "key": "category",
                "regex": r"Category:([^\n]+)\n\s*\n"
            },
            {
                "key": "rights_information",
                "regex": r"Rights Information:([\S\s]+)"
            }
        ]

    def split_description(self):
        description = self.source_description
        description_parts = {}

        for field_configuration in self.get_mapping_configuration():
            matches = re.search(field_configuration.get("regex"),
                                self.source_description, re.MULTILINE)
            if not matches:
                continue

            description = description.replace(matches.group(0), "")

            if not field_configuration.get("keep", True):
                continue

            description_parts.update({field_configuration.get("key"):
                                      matches.group(1).strip()})

        description_parts.update({"description": description})

        return description_parts

    def map_subject(self):
        subjects = [tag.get("raw") for tag
                    in self.source_metadata.get("tags", {}).get("tag", [])]

        category = self.split_description().get("category")

        if category:
            subjects.append(category.lower())

        return subjects

    def map_type(self):
        return self.split_description().get("type")

    def map_provenance(self):
        return self.split_description().get("provenance")

    def map_rights(self):
        return self.split_description().get("rights_information")

    def map_date(self):
        return self.split_description().get("date")

    def map_description(self):
        return self.split_description().get("description")

    def map_identifier(self):
        """
        Combine `previous_identifier` and `identifier` values from description
        metadata. The `previous_identifier` may contain an ARK, which we
        extract.
        """
        previous_identifiers = self.split_description().\
            get("previous_identifier", "")
        identifiers = [self.split_description().get("identifier")]

        for previous_identifier in previous_identifiers.split(" / "):
            if "ark:" in previous_identifier:
                matches = re.search(r"ark:[\/a-z0-9]+", previous_identifier)
                if matches:
                    identifiers.append(matches[0])
            else:
                identifiers.append(previous_identifier)

        return identifiers


class SpplVernacular(FlickrVernacular):
    record_cls = SpplRecord

