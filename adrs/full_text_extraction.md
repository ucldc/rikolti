# Full Text Extraction
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

We need a component that will take a content file (pdf or image) and run text extraction on that content file so we can add more words to our search index. 

### Decision

Decided to use AWS Textract for now.

### Status

Decided for the sake of the demo, may switch to running [textract](https://textract.readthedocs.io/en/stable/index.html) at a later date. 

## Details

### Assumptions

Getting full text indexing will give us better search results. If we include the full text in the HTML of the page, it may also aid our SEO efforts to demonstrate uniqueness across items with minimal metadata, but substantial words in the content file. 

### Constraints

We cannot run the textract python package in AWS Lambda due to a number of required source libraries. AWS Lambda wants pure python projects only. 

### Positions

We are wanting to run Pachamama as in a serverless framework. 

AWS Textract is actually just ec2 machines under the hood, but we can interact with AWS Textract as a managed, serverless service. 

### Argument

While we could set up some worker management for full text extraction, the time it would take seems particularly large given how cheap AWS textract is and how early in development we currently are. 

### Implications

Cost of using AWS Textract Detect Document Text API is $1.50 per 1,000 pages for the first 1 million pages, and $0.60 per 1,000 pages over a million pages. An image counts as 1 page, but an 8 page PDF is 8 pages. 

We do not need the more expensive Analyze Document API. 

## Related

### Related Decisions

Use an AWS Lambda to kick off a job for each content file. 

Use Amazon Simple Notification Service for AWS Textract to notify us when a job is complete. 

Use an AWS Lambda subscribed to the notification channel to process the results of the job. 

### Related Requirements

### Related Artifacts

### Related Principles

## Notes

To kick off the job, the AWS Lambda function only needs to list the contents of a bucket, and create a job for each file. This can easily be done in 15 minutes, so the `start_textract` lambda doesn't need to use asyncio. 

To process the results of the job, the AWS Lambda function is only working with one record at a time, which can easily be done in 15 minutes, so the `get_textract` lambda also doesn't need to use asyncio. 
