## Create OpenSearch Cluster

To create an AWS OpenSearch domain and associated AWS resources for a particular environment (dev, stg or prod):

```
python cloudformation/create_opensearch_stack.py <env>
```

This will create an AWS CloudFormation stack named `rikolti-opensearch-<env>-<date-time>`, and print output on how the job went.

The `create_opensearch_stack.py` script runs `scripts/create_cf_template_opensearch.py`. This is a [troposphere](https://troposphere.readthedocs.io/en/latest/) script, which creates a cloudformation template. The template is then used as the basis for creating the stack.

## Create OpenSearch index template

To create the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html) for rikolti:

```
python index_templates/rikolti_template.py
```

This creates a template that will be used whenever an index with name matching `rikolti*` is added to the cluster.

## Index Records

NOTE: What's in the repo right now was thrown together quickly and definitely needs more thought.

Right now I'm assuming we want to bulk add records to the index rather than launching a separate Lambda for each record and adding one record to the index at a time. However, each record is currently stored as an individual json file on S3, which necessitates the extra step of assembling the metadata for multiple records into a list. Perhaps this assembly step negates the efficiency of bulk adding the records to the index. Also, if the collection is very large, then we may run into the 15 minute limit for Lambda. Lambda probably isn't the best solution for this.

Options:

1. batch load records using something other than Lambda (AWS Batch with Fargate?) 
2. launch one Lambda per record
3. launch one Lambda per batch of records



