# Server Infrastructure
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

We need a not-insignificant amount of computing power, storage, permissions, and system coordination to perform harvesting tasks. We are considering Amazon Web Services.

- We want the developer and operator experience to be fast and reliable, with useful error messaging, both for initial setup and ongoing use. 
- We want strong support that is easily accessible. 
- We want to minimize our management time and costs for our server infrastructure.
- We want to be able to swap out different pieces of infrastructure arbitrarily as cheaper, faster, or better alternatives become available. 
- We want to consider Amazon Web Services for hosting Pachamama. 

### Decision

Decided to use Amazon Web Services. 

### Status

Decided. Open to revisiting if/when new significant information arrives. 

## Details

### Assumptions

Keeping Pachamama fast and reliable will allow operators to run harvests quickly, with more focus on metadata mappings, and less on infrastructure-related bugs. 

We have key support members in the UCOP dedicated AWS representative as well as the IAS team. 

Managing worker images as well as worker instances in our current setup can be time consuming, and at times inefficient, as when a worker is left running but there are no jobs left to perform. 

### Constraints

If we choose lower level infrastructure options, we will have to spend significantly more time installing our own or vendor-built components, implementing coordination between components, and tuning our server infrastructure to optimize those components. 

### Positions

We did not consider many other options, given CDL's relationship with AWS, our on-team AWS experience, the IAS team's AWS experience, as well as having a dedicated AWS rep. 

Amazon Web Services is fast and reliable. 

Beyond the AWS rep and knowledgable IAS team, AWS documentation is generally quite thorough and useful. 

AWS provides many infrastructure requirements such as role management, monitoring, queuing, notifications, storage, text extraction, indexing and workflow coordination, as well as lower level server instances and clusters. 

AWS services play well with one another, and there are many documented use cases of interacting across several different AWS services. 

### Argument

Amazon Web Services provides many different services, which will allow us to develop a new harvest pipeline relatively quickly atop some of their higher level services while maintaining the flexibility to migrate components to their lower level services later. For example, we can use AWS Textract to quickly get text extraction of documents up and running, and later swap out AWS Textract with an AWS ec2 instance running text extraction software at a later date, should that be a worthwhile decision. 

### Implications

We are diving further into the AWS ecosystem. 

## Related

### Related Decisions

Given our decision to use Amazon Web Services, we have decided to use IAM roles, s3 storage, AWS Lambda, Glue, AWS ElasticSearch, AWS Textract, and AWS Simple Notification Service. 

### Related Requirements

### Related Artifacts

https://drive.google.com/file/d/1TwDZ8k6S2gDc8YfA2JdLtJ4E5uh_jXDm/view?usp=sharing

### Related principles

## Notes


