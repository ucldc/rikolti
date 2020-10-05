# Serverless
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

We want to work with a serverless framework as much as possible to reduce overhead in managing worker instances. 

- We want the developer and operator experience to be fast and reliable, with useful error messaging, both for initial setup and ongoing use. 
- We want to minimize our management time and costs for our server infrastructure.
- We want to be able to swap out different pieces of infrastructure arbitrarily as cheaper, faster, or better alternatives become available. 

### Decision

We've decided to use AWS Lambda and AWS Glue as our serverless platforms of choice. 

### Status

Decided. Open to revisiting if/when new significant information arrives. 

## Details

### Assumptions

Serverless will help us to focus on the code base, rather than on maintaining infrastructure. It will be easier to develop locally in a serverless framework. 

### Constraints

Managing worker images as well as worker instances in our current setup can be time consuming, and at times inefficient, as when a worker is left running but there are no jobs left to perform. 

If we choose to run workers again, we will have to spend significantly more time installing our own or vendor-built components, implementing coordination between components, and tuning our server infrastructure to optimize those components. 

### Positions

We have key support members in the UCOP dedicated AWS representative as well as the IAS team, and AWS is well documented. AWS is also fast and reliable. AWS services play well with one another, and there are many documented use cases of interacting across several different AWS services. 

Lambda and Glue are both a pay-what-you-use model, and the clusters are all maintained for us. 

### Argument

AWS Glue and AWS Lambda will re-inforce the modular architecture we're wanting to develop for Pachamama. Furthermore, since we will not have to manage or maintain worker images or servers to do the harvesting work, we will be able to develop a new harvest pipeline relatively quickly atop these two frameworks. All while maintaining the flexibility to migrate components to their lower level services later in order to, for example, run our own Spark cluster. 

### Implications

## Related

### Related Decisions

### Related Requirements

### Related Artifacts

### Related Principles

## Notes
