from .flickr_mapper import FlickrRecord, FlickrVernacular
import re


class SdasmRecord(FlickrRecord):
    def UCLDC_map(self):
        return {
            "description": self.map_description,
            "identifier": self.map_identifier
        }

    @property
    def source_description(self):
        return self.source_metadata.get("description", {}).get("_content", "")

    @staticmethod
    def get_mapping_configuration():
        """
        Provides configuration for locating and extracting metadata values from
        the description field using regex. The "key" field gives the regex a
        name. The capture group in the regex is the value that we extract for
        that field. The entire match (including the parts outside the capture
        group), are replaced with an empty string in the description field.

        There are two formats that SDASM uses. One separates values with " - ".
        The other separates with linebreaks. The fields that use these two
        formats don't appear to mix fields, which is why this works. It might be
        possible to match both formats, but doesn't seem necessary at this time.
        """
        return [
            {
                "key": "piction_id",
                "regex": r"(^| )PictionID: ?(\S+)"
            },
            {
                "key": "catalog",
                "regex": r"(^| )Catalog: ?(\S+)"
            },
            {
                "key": "filename",
                "regex": r"(^| )Filename: ?(\S+)"
            },
            {
                "key": "date_on_neg",
                "regex": r"(^| )Date on Neg: ?(\S+)"
            },
            {
                "key": "year",
                "regex": r"(^| )Year: ?([^\n]+)\n"
            },
            {
                "key": "date",
                "regex": r"(^| )Date: ?(\S+)",
            },
            {
                "key": "sdasm_catalog",
                "regex": r"^SDASM Catalog #: ?([^\n]+)\n"
            },
            {
                "key": "corp_name",
                "regex": r"^Corp. Name: ?([^\n]+)\n"
            },
            {
                "key": "title",
                "regex": r"^Title: ?([^\n]+)\n"
            },
            {
                "key": "catalog_or_negative_number",
                "regex": r"^Catalog or Negative #: ?([^\n]+)\n"
            },
            {
                "key": "media_negative_size",
                "regex": r"^Media +\(negative size\): ?([^\n]+)\n"
            },
            {
               "key": "description",
               "regex": r"Description: ?([^\n]*)\n"
            },
            {
                "key": "repository",
                "regex": r"Repository:(</b>)? ?([^\n]*)$"
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

            description_parts.update({field_configuration.get("key"):
                                     matches.groups()[-1].strip()})

        # Set the description if it wasn't provided as metadata in the
        # description field
        if "description" not in description_parts:
            description_parts.update({"description": description})

        return description_parts

    def map_date(self):
        return list(filter(None, [
            self.split_description().get("date"),
            self.split_description().get("date_on_neg"),
            self.split_description().get("year")
        ]))

    def map_identifier(self):
        return list(set(filter(None, [
            self.split_description().get("piction_id"),
            self.split_description().get("catalog"),
            self.split_description().get("filename"),
            self.split_description().get("sdasm_catalog"),
            self.split_description().get("catalog_or_negative_number")
        ])))

    def map_description(self):
        description = self.split_description().get("description")

        # Get rid of the message wrapped in triple dashes at the end. This only
        # works if the repository field is already extracted.
        description = re.sub(r"---[^-]+---$", "", description)

        # Get rid of multiple " -" which serve as separators
        description = re.sub(r"( +-){2,}", " -", description)

        # Get rid of an initial " -" if one exists
        description = re.sub(r"\A -", "", description, re.MULTILINE)

        # Extracting the title -- from "Title:" to " - ", is possible -- but
        # would require some close analysis to review the results.
        return description


class SdasmVernacular(FlickrVernacular):
    record_cls = SdasmRecord
