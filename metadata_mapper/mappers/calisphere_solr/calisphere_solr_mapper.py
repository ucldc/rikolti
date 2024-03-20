import json

from ..mapper import Record, Vernacular, Validator

class CalisphereSolrRecord(Record):
    # This mapper does not handle Nuxeo record complexities, meaning:
    #   - it ignores structmap* solr fields for complex objects
    #   - it does not map media_source
    def UCLDC_map(self) -> dict:
        return {
            "calisphere-id": self.map_calisphere_id(),
            "is_shown_at": self.source_metadata.get("url_item"),
            "thumbnail_source": self.map_thumbnail_source(),
            "title": self.source_metadata.get("title"),
            "alternative_title": self.source_metadata.get("alternative_title", None),
            "contributor": self.source_metadata.get("contributor", None),
            "coverage": self.source_metadata.get("coverage", None),
            "creator": self.source_metadata.get("creator", None),
            "date": self.source_metadata.get("date", None),
            "facet_decade": self.source_metadata.get('facet_decade', None),
            "extent": self.source_metadata.get("extent", None),
            "format": self.source_metadata.get("format", None),
            "genre": self.source_metadata.get("genre", None),
            "identifier": self.source_metadata.get("identifier", None),
            "language": self.source_metadata.get("language", None),
            "location": self.source_metadata.get("location", None),
            "publisher": self.source_metadata.get("publisher", None),
            "relation":self.source_metadata.get("relation", None),
            "rights": self.source_metadata.get("rights", None),
            "rights_holder": self.source_metadata.get("rights_holder", None),
            "rights_note": self.source_metadata.get("rights_note", None),
            "rights_date": self.source_metadata.get("rights_date", None),
            "source": self.source_metadata.get("source", None),
            "spatial": self.source_metadata.get("spatial", None),
            "subject": self.source_metadata.get("subject", None),
            "temporal": self.source_metadata.get("temporal", None),
            "type": self.source_metadata.get("type", None),
            "sort_title": self.source_metadata.get("sort_title", None),
            "description": self.source_metadata.get("description", None),
            "provenance": self.source_metadata.get("provenance", None),
            "transcription": self.source_metadata.get("transcription", None),
            "id": self.source_metadata.get("id", None),
            "campus_name": self.source_metadata.get("campus_name", None),
            "campus_data": self.source_metadata.get("campus_data", None),
            "campus_url": self.parse_url_for_id(
                self.source_metadata.get("campus_url", [])),
            "collection_name": self.source_metadata.get("collection_name", None),
            "collection_data": self.source_metadata.get("collection_data", None),
            "collection_url": self.parse_url_for_id(
                self.source_metadata.get("collection_url", [])),
            "sort_collection_data": self.source_metadata.get("sort_collection_data", None),
            "repository_name": self.source_metadata.get("repository_name", None),
            "repository_data": self.source_metadata.get("repository_data", None),
            "repository_url": self.parse_url_for_id(
                self.source_metadata.get("repository_url", [])),
            "rights_uri": self.source_metadata.get("rights_uri", None),
            "sort_date_start": self.source_metadata.get("sort_date_start", None),
            "sort_date_end": self.source_metadata.get("sort_date_end", None),
        }
    
    def parse_url_for_id(self, url_values: list[str]) -> list[str]:
        return [url_value.split('/')[-1] for url_value in url_values]

    def map_calisphere_id(self):
        harvest_id = self.source_metadata['harvest_id_s']
        return harvest_id.split("--")[1]

    def map_thumbnail_source(self):
        image_md5 = self.source_metadata.get("reference_image_md5", None)
        if image_md5:
            return f"https://static-ucldc-cdlib-org.s3.us-west-2.amazonaws.com/harvested_images/{image_md5}"

class CalisphereSolrValidator(Validator):
    def setup(self):
        self.remove_validatable_field(field="is_shown_by")

class CalisphereSolrVernacular(Vernacular):
    record_cls = CalisphereSolrRecord
    validator = CalisphereSolrValidator

    def parse(self, api_response):
        page_element = json.loads(api_response)
        records = page_element.get("response", {}).get("docs", [])
        return self.get_records([record for record in records])
