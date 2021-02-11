# Metadata Record and Content File Derivative Storage
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

We need a secure and reliable place to store metadata records and content file derivatives. This storage needs to:
- provide quick and reliable access to records and content files while working on the records throughout the Pachamama workflow. 
- reliably store the end results of the workflow. 
- interoperate easily with all the Pachamama components. 

### Decision

We decided to use AWS s3 as Pachamama's metadata record and content file derivative storage. Metadata records are stored in json line format in s3. 

### Status

Decided. Open to revisiting if/when new significant information arrives. 

## Details

### Assumptions

Metadata records can be treated as atomic entities, they are not highly relational and do not need a relational database such as MySQL or PostgreSQL. 

### Constraints

We will need a metadata record storage structure that allows us to infer what little relational data we need: collection ID, date record was created, last date modified. 

### Positions

We currently use CouchDB to store metadata records, as well as harvest process meta metadata, and s3 to store content file derivatives. 

Couch has repeatedly given us issues, from cryptic 'Bad Connection' errors that propagate poorly through our existing system and can be red herrings for other issues, to anxiety about our team's ignorance in how CouchDB was initially set up and how to triage issues if CouchDB goes down. 

We looked into s3 as a data storage solution due to a lack of expertise in working with CouchDB, an unclear use case for using a database at all, existing expertise with s3, and existing usage of s3 as storage for content file derivatives. 

### Argument

With s3 prefixing, we can store what little relational data is necessary for each record (namely, the collection ID). 

s3 versioning allows us to track multiple versions of the vernacular metadata document, allowing us to roll-back to using a previous version of the record as needed. 

s3 bucket policies ensures the records are written and read only by the necessary Pachamama components using IAM roles. 

s3 bucket policies also will allow us to easily trash out-dated s3 documents. 

s3 is quick and easy to access, does not require the same tuning that a database would, nor the same maintenance of keeping the database up, running, and updated. 

### Implications

Every json record must have a unique calisphere-id in the record, and must exist within a prefix containing the collection ID. 

The input and output of every Pachamama component is an s3 json or json line file. 

While records can be treated as atomic units, json line files cannot, so there are some paging implications as well. Textract, for example, creates an individual json file per record, while the metadata fetcher creates a json line file per page from the institution's API. 

## Related

### Related Decisions

### Related Requirements

### Related Artifacts

### Related Principles

## Notes
