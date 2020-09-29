# Vernacular Metadata Fetcher
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

We need a component that hits an institution's API to retrieve metadata records. 

### Decision

We decided to build this component in python, deploy it to AWS Lambda, and store vernacular metadata records in s3. 

### Status

Decided. Open to revisiting if/when new significant information arrives. 

## Details

### Assumptions

Institutional APIs can be slow, and not optimized for crawling, so it is ideal to decouple this metadata fetcher component from all other Pachamama components. 

Having the vernacular metadata stored 'locally' will allow for quick and rapid development and review cycles of the mappings for any given set of records. 

Fetching takes place on a per-collection resolution. 

### Constraints

### Positions

### Argument

### Implications

## Related

### Related Decisions

### Related Requirements

### Related Artifacts

### Related Principles

## Notes
