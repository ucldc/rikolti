from ..mapper import Record, Vernacular, Validator
import json
from typing import Any
import re


class FlickrRecord(Record):
    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "date": self.map_date,
            "description": self.map_description,
            "format": ["Photo"],
            # "identifier": [self.source_metadata.get("id")],
            "spatial": self.map_spatial,
            "subject": self.map_subject,
            "title": self.map_title,
            "type": ["Image"]
        }

    def map_is_shown_at(self):
        urls = self.source_metadata.get("urls", {}).get("url", [])

        for url in urls:
            if url.get("type") == "photopage":
                return url.get("_content")

    @property
    def url_image(self):
        """
        See: https://www.flickr.com/services/api/misc.urls.html
        """

        server = self.source_metadata.get("server")
        id = self.source_metadata.get("id")
        secret = self.source_metadata.get("secret")

        return f"https://live.staticflickr.com/{server}/{id}_{secret}_z.jpg"

    def map_is_shown_by(self):
        return self.url_image

    def map_date(self):
        """
        Note from legacy mapper:
        It appears that the "taken" date corresponds to the date uploaded
        if there is no EXIF data. For items with EXIF data, hopefully it is
        the date taken == created date.
        """
        pass

    def map_description(self):
        return [self.source_metadata.get("description", {}).get("_content")]

    def map_subject(self):
        tags = self.source_metadata.get("tags", {}).get("tag", [])
        return [{"name": tag.get("raw")} for tag in tags]

    def map_title(self):
        return [self.source_metadata.get("title", {}).get("_content")]

    def map_format(self):
        return self.source_metadata.get("media")

    def map_spatial(self):
        """
        Comment from legacy mapper: Some photos have spatial (location) data

        It looks like the only location may be the location of the owner of
        the photo, as it's an attribute on the owner object
        """
        pass


class FlickrValidator(Validator):
    def __init__(self, **options):
        super().__init__(**options)
        self.add_validatable_field(
            field="is_shown_by", type=str,
            validations=[
                Validator.required_field,
                Validator.type_match,
                FlickrValidator.content_match_regex
            ]
        )

    @staticmethod
    def content_match_regex(validation_def: dict, rikolti_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is equal.

        If comparison_value is in the old flickr url style, replace with new
        flickr url style and compare; if not, then compare the value as-is
        """
        if comparison_value:
            old_flickr_url_template = (
                r"https://farm\d+.staticflickr.com/(\d+)/([\d_\w]+).jpg"
            )
            match = re.fullmatch(old_flickr_url_template, comparison_value)
            if match:
                comparison_value = (
                    f"https://live.staticflickr.com/{match.group(1)}/"
                    f"{match.group(2)}.jpg"
                )

        if not validation_def["validation_mode"].value.compare(
            rikolti_value, comparison_value):
            return "Content mismatch"


class FlickrVernacular(Vernacular):
    record_cls = FlickrRecord
    validator = FlickrValidator

    def parse(self, api_response):
        def modify_record(record):
            record.update({"calisphere-id": f"{self.collection_id}--"
                                            f"{record.get('id')}"})
            return record

        records = [modify_record(record) for record in json.loads(api_response)]
        return self.get_records(records)
