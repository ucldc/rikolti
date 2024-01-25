class CalisphereSolrRecord(Record):

    def UCLDC_map(self) -> dict:
        return {
            "calisphere-id": self.source_metadata.get('id'),
            #"is_shown_at": self.source_metadata.get("url_item"),
            #"is_shown_by": # same as thumbnail_source
            #"media_source":
            #"thumbnail_source":
            "title": self.source_metadata.get("title"),
            "alternative_title": self.source_metadata.get("alternative_title", None),
            "contributor": self.source_metadata.get("contributor", None),
            "coverage": self.source_metadata.get("coverage", None),
            "creator": self.source_metadata.get("creator", None),
            "date": self.source_metadata.get("date", None),
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
            "subject": self.source_metadata.get("subject", None)
            "temporal": self.source_metadata.get("temporal", None),
            "type": self.source_metadata.get("type", None),
            "sort_title": self.source_metadata.get("sort_title", None),
            "description": self.source_metadata.get("description", None),
            "provenance": self.source_metadata.get("provenance", None),
            "transcription": self.source_metadata.get("transcription", None),
            "id": self.source_metadata.get("id", None),
            "campus_name": self.source_metadata.get("campus_name", None),
            "campus_data": self.source_metadata.get("campus_data", None),
            "collection_name": self.source_metadata.get("collection_name", None),
            "collection_data": self.source_metadata.get("collection_data", None),
            "collection_url": self.source_metadata.get("collection_url", None),
            "sort_collection_data": self.source_metadata.get("sort_collection_data", None),
            "repository_name": self.source_metadata.get("repository_name", None),
            "repository_data": self.source_metadata.get("repository_data", None),
            "repository_url": self.source_metadata.get("repository_url", None),
            "rights_uri": self.source_metadata.get("rights_uri", None),
            "manifest": self.source_metadata.get("manifest", None),
            "object_template": self.source_metadata.get("object_template", None),
            "url_item": self.source_metadata.get("url_item", None),
            "created": self.source_metadata.get("created", None),
            "last_modified": self.source_metadata.get("last_modified", None),            
            "sort_date_start": self.source_metadata.get("sort_date_start", None),
            "sort_date_end": self.source_metadata.get("sort_date_end", None),
            "campus_id": self.source_metadata.get("campus_id", None),
            "collection_id": self.source_metadata.get("collection_id", None),
            "repository_id": self.source_metadata.get("repository_id", None),
            "item_count": self.source_metadata.get("item_count", None),


            """
            # fields added later by content harvester?
            "media": ,
            "thumbnail": 

            # some nuxeo objects have a reference image
            "reference_image_md5": self.source_metadata.get("reference_image_md5", None),
            "reference_image_dimensions": self.source_metadata.get("reference_image_dimensions", None),
            
            # some nuxeo objects have children
            "children": 
            """         
        }
        

class CalisphereSolrVernacular(Vernacular):
    record_cls = CalisphereSolrRecord
    # validator = CalisphereSolrRecordValidator

