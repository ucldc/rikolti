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

Fetching can take a variable length of time, pending the size of the colllection and the speed of the institutional API. 

Fetching should be able to fetch from a variety of different endpoints and should only do the bare minimum of data processing: getting the records all in json line formatting. 

Pagination of the different harvest endpoints needs to be taken into account. 

Needs to write vernacular metadata json line format documents into s3. 

### Positions

Rather than managing ec2 instances, we'd prefer a managed, serverless solution. 

Our team has existing expertise in Python development.

### Argument

AWS Lambda is a fully managed serverless service that fits our use case quite nicely and supports Python. Using AWS Lambda has one challenge: AWS Lambdas timeout after 15 minutes. We can get around this, though, by setting a timeout in our code at, say, 14 minutes, which gives us enough time to kick off a new Lambda before the first one dies. 

### Implications

We need to use Python asyncio to get python timeout functionality with the [coroutine asyncio.wait_for(_awaitable_, _timeout_, _/*_ , _loop=None_)](https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for) function. Using asyncio, however, means the rest of the fetcher function must be written in async python. 

## Related

### Related Decisions

We need to use aioboto3 and aiohttp python modules for making async API calls and async s3 calls. 

Aside from pulling in the strictly vernacular metadata record (or as strictly as we can, having converted to json), we need to create an ID for each object right off the bat. See the ADR for [parallel data processing](/parallel-data-processing.md)

### Related Requirements

Lambda parameters must be stateful: they need to keep track of the page number or resumption token, so that the next Lambda knows where to pick up where the last Lambda left off. 

### Related Artifacts

### Related Principles

State management/idempotent lambdas

## Notes

Code for this component lives at `metadata_fetcher`. 

`metadata_fetcher/lambda_function.py` handles determining which style of fetcher to use, initializing the Fetcher object, and takes care of timeout handling. The AWS Lambda function takes a json object such as: 

```json
{
    "collection_id": 466,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
        "path": "/asset-library/UCSF/MSS 2000-31 AIDS Ephemera Collection/"
    }
}
```

`metadata_fetcher/Fetcher.py` handles the aiohttp and aioboto3 coordination, while the subclasses `OAIFetcher` and `NuxeoFetcher` implementions must implement the following methods: 

`build_fetch_request` includes details about how to create the aiohttp parameters for the next page of results from the Fetcher's internal state; returns None when there are no pages left. 
`get_records` includes details about how to process a single page of records from the API and returns an array of records represented in dictionary format. 
`increment` includes details about how to process the HttpResp from the API to increment the Fetcher's internal state to get the next page.
`build_id` includes details about how to build the universal Calisphere ID for each record.  
`json` dumps the internal state of the fetcher as a json object to send to the next lambda. 