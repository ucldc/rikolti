[Description about ADRs & examples](https://github.com/joelparkerhenderson/architecture_decision_record)

# Search Index
Contents:
- [Summary](#summary)
	- [Issue](#issue)
	- [Decision](#decision)
	- [Status](#status)
- [Details](#details)
	- [Assumptions](#assumptions)
	- [Constraints](#constraints)
	- [Positions](#positions)
	- [Argument](#argument)
	- [Implications](#implications)
- [Related](#related)
	- [Related decisions](#related-decisions)
	- [Related requirements](#related-requirements)
	- [Related artifacts](#related-artifacts)
	- [Related principles](#related-principles)
- [Notes](#notes)

## Summary

### Issue

We need an index and search engine to power the front-end Calisphere site.

### Decision

We went with AWS ElasticSearch for the proof-of-concept.

### Status

The proof-of-concept was successful. We created a searchable ES index based on the transformed metadata output from Glue/Spark, and created a simple Web UI to display it.

## Details

### Assumptions

We need a modern, reliable, fast indexing and search engine solution to power the Calisphere website. This might also serve as the API for Calisphere in general.

### Constraints

We don't have ES expertise on our team, although Brian is well-versed in Lucene based search engines in general.

### Positions

It is worth it to learn a new search engine technology, i.e. ElasticSearch, for its improvements over Solr and especially its distributed full-text search capabilities.

### Argument

See above. There is also an AWS ElasticSearch offering, which allows us to use AWS to more easily set up and manage the ES domain infrastructure.

### Implications



## Related

### Related Decisions

### Related Requirements

### Related Artifacts

### Related Principles

## Notes
