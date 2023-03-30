# Create OpenSearch Cluster

To create an AWS OpenSearch domain and associated AWS resources for a particular environment (dev, stg or prod):

```
python cloudformation/create_opensearch_stack.py <env>
```

This will create an AWS CloudFormation stack named `rikolti-opensearch-<env>-<date-time>`, and print output on how the job went.

The `create_opensearch_stack.py` script runs `scripts/create_cf_template_opensearch.py`. This is a [troposphere](https://troposphere.readthedocs.io/en/latest/) script, which creates a cloudformation template. The template is then used as the basis for creating the stack.


