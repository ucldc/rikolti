RECORD_INDEX_CONFIG = {
    "index_patterns": ["rikolti*"],
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 2,
            "analysis": {
                "normalizer": {
                    "lowercase_trim": {
                        "filter": ["trim", "lowercase"]
                    }
                },
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_keywords": {
                        "type": "keyword_marker",
                        "keywords": ["example"]
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    }
                },
                "analyzer": {
                    "asciifolded_english": {
                        "tokenizer": "standard",
                        "filter": [
                            "english_possessive_stemmer",
                            "lowercase",
                            "asciifolding",
                            "english_stop",
                            "english_keywords",
                            "english_stemmer"
                        ]
                    }
                },
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "title": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "alternative_title": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "contributor": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "coverage": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "creator": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "date": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "extent": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "format": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "genre": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "identifier": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "language": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "location": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "publisher": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "relation": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "rights": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "rights_holder": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "rights_note": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "rights_date": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "source": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "spatial": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "subject": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "temporal": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},
                "type": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},

                "sort_title": {"type": "keyword", "normalizer": "lowercase_trim"},
                "facet_decade": {"type": "text", "analyzer": "asciifolded_english", "fields": {"raw": {"type": "keyword"}}},

                "description": {"type": "text", "analyzer": "asciifolded_english"},
                "provenance": {"type": "text", "analyzer": "asciifolded_english"},
                "transcription": {"type": "text", "analyzer": "asciifolded_english"},

                "calisphere-id": {"type": "keyword"},
                "id": {"type": "keyword"},
                "campus_name": {"type": "keyword"},
                "campus_data": {"type": "keyword"},
                "campus_url": {"type": "keyword"},
                "campus_id": {"type": "alias", "path": "campus_url"},
                "collection_name": {"type": "keyword"},
                "collection_data": {"type": "keyword"},
                "collection_url": {"type": "keyword"},
                "collection_id": {"type": "alias", "path": "collection_url"},
                "sort_collection_data": {"type": "keyword"},
                "repository_name": {"type": "keyword"},
                "repository_data": {"type": "keyword"},
                "repository_url": {"type": "keyword"},
                "repository_id": {"type": "alias", "path": "repository_url"},
                "rights_uri": {"type": "keyword"},
                "url_item": {"type": "keyword"},
                "fetcher_type": {"type": "keyword"},
                "mapper_type": {"type": "keyword"},
                "rikolti": {
                    "properties": {
                        "indexed_at": {"type": "date"},
                        "version_path": {"type": "keyword"},
                        "page": {"type": "keyword"},
                    }
                },

                "sort_date_start": {"type": "date"},
                "sort_date_end": {"type": "date"},

                "media": {
                    "properties": {
                        "mimetype": {"type": "keyword"},
                        "path": {"type": "keyword"},
                        "format": {"type": "keyword"},
                    }
                },
                "thumbnail": {
                    "properties": {
                        "mimetype": {"type": "keyword"},
                        "path": {"type": "keyword"},
                        "dimensions": {"type": "keyword"}
                    }
                },

                "children": {"type": "nested"}
            }
        }
    }
}
