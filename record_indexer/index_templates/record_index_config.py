RECORD_INDEX_CONFIG = {
    "index_patterns": ["rikolti*"],
    "template": {
        "settings": {
            "number_of_shards": 1,
            "analysis": {
                "analyzer": {
                    "keyword_lowercase_trim": {
                        "tokenizer": "keyword",
                        "filter": ["trim", "lowercase"]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "alternative_title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "contributor": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "coverage": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "creator": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "date": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "extent": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "format": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "genre": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "identifier": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "language": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "location": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "publisher": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "relation": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "rights": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "rights_holder": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "rights_note": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "rights_date": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "source": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "spatial": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "subject": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "temporal": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
                "type": {"type": "text", "fields": {"raw": {"type": "keyword"}}},

                "sort_title": {"type": "text", "analyzer": "keyword_lowercase_trim"},
                "facet_decade": {"type": "text", "fields": {"raw": {"type": "keyword"}}},

                "description": {"type": "text"},
                "provenance": {"type": "text"},
                "transcription": {"type": "text"},

                "calisphere-id": {"type": "keyword"},
                "id": {"type": "keyword"},
                "campus_name": {"type": "keyword"},
                "campus_data": {"type": "keyword"},
                "campus_url": {"type": "keyword"},
                "collection_name": {"type": "keyword"},
                "collection_data": {"type": "keyword"},
                "collection_url": {"type": "keyword"},
                "sort_collection_data": {"type": "keyword"},
                "repository_name": {"type": "keyword"},
                "repository_data": {"type": "keyword"},
                "repository_url": {"type": "keyword"},
                "rights_uri": {"type": "keyword"},
                "url_item": {"type": "keyword"},
                "fetcher_type": {"type": "keyword"},
                "mapper_type": {"type": "keyword"},

                "sort_date_start": {"type": "date"},
                "sort_date_end": {"type": "date"},

                "media": {
                    "properties": {
                        "media_filepath": {"type": "keyword"},
                        "mimetype": {"type": "keyword"},
                        "path": {"type": "keyword"}
                    }
                },
                "media_source": {
                    "properties": {
                        "filename": {"type": "keyword"},
                        "mimetype": {"type": "keyword"},
                        "nuxeo_type": {"type": "keyword"},
                        "url": {"type": "keyword"}
                    }
                },
                "thumbnail": {
                    "properties": {
                        "mimetype": {"type": "keyword"},
                        "thumbnail_filepath": {"type": "keyword"},
                        "path": {"type": "keyword"},
                        "dimensions": {"type": "keyword"}
                    }
                },
                "thumbnail_source": {
                    "properties": {
                        "filename": {"type": "keyword"},
                        "mimetype": {"type": "keyword"},
                        "nuxeo_type": {"type": "keyword"},
                        "url": {"type": "keyword"}
                    }
                },

                "children": {"type": "nested"}
            }
        }
    }
}
