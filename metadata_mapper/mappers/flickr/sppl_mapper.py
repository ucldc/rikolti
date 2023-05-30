from .flickr_mapper import FlickrRecord, FlickrVernacular
import re


class SpplRecord(FlickrRecord):
    def UCLDC_map(self):
        split_description = self.split_description()
        return {
            "description": split_description.get("description"),
            "identifier": self.map_identifier(split_description),
            "type": split_description.get("type"),
            "rights": split_description.get("rights_information"),
            "provenance": [split_description.get("provenance")],
            "subject": self.map_subject(split_description),
            "date": split_description.get("date")
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

    def map_subject(self, split_description):
        subjects = [{"name": tag.get("raw")} for tag
                    in self.source_metadata.get("tags", {}).get("tag", [])]

        category = split_description.get("category")

        if category:
            subjects.append({"name": category.lower()})

        return subjects

    def map_identifier(self, split_description):
        """
        Combine `previous_identifier` and `identifier` values from description
        metadata. The `previous_identifier` may contain an ARK, which we
        extract.
        """
        previous_identifiers = split_description.get("previous_identifier", "")
        identifiers = [split_description.get("identifier")]

        for previous_identifier in previous_identifiers.split(" / "):
            if "ark:" in previous_identifier:
                matches = re.search(r"ark:[\/a-z0-9]+", previous_identifier)
                if matches:
                    identifiers.append(matches[0])
            else:
                identifiers.append(previous_identifier)

        return [i for i in identifiers if i != "N/A"]


class SpplVernacular(FlickrVernacular):
    record_cls = SpplRecord

