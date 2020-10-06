
# Metadata Transformation
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

We need a technology to transform the disparate metadata that we ingest into a single UCLDC schema.

### Decision

We decided to use AWS Glue for now

### Status

We have created a couple of proof-of-concept data transforms for the demo on 10/07/2020 using AWS Glue, and it looks like it will work well.

## Details

### Assumptions

Data transformation is a core component of the harvesting infrastructure. We want something fast and powerful. Apache Spark, which is the core processing technology around which AWS Glue is built, is known to be fast and powerful.

### Constraints

We will need to get fully up to speed on using Apache Spark for data transformation. It takes some time to learn, and nobody on our team knows it well yet. 

Some of the code (UDFs, or user defined functions) should be written in scala, as starting a python process is expensive and it is moreover very costly to serialize the data to python.

### Positions

As mentioned above, we don't have expertise in Spark or the Glue framework, but after having done a proof-of-concept, we think it is worth the overhead of getting up to speed on it. There is PySpark, so we can write the ETL scripts in python. We will need to learn a bit of Scala. 

### Argument

Despite needing to learn Spark and Glue (the former being more difficult to learn than the latter), we think it will be worth it for the speed.

### Implications

## Related

### Related Decisions

### Related Requirements

### Related Artifacts

### Related Principles

## Notes
