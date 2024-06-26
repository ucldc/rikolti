# Indexing

We push all records that have been run through the Rikolti harvesting pipeline into an OpenSearch index. 

Records must adhere strictly to the fields specified [our index template](index_templates/record_index_config.py). Please review [documentation from opensearch on index templates](https://opensearch.org/docs/latest/im-plugin/index-templates/) for more information on index templates. 

Our `record_indexer` component is designed to remove any fields that are not in our index template. The `record_indexer` indexes records by collection into indicies identified by aliases. 

## Configuring the Record Indexer - AWS and Docker Options

The Record Indexer indexes records by hitting the configured `OPENSEARCH_ENDPOINT` - the API endpoint for an opensearch instance. Rikolti supports authenticating against an AWS hosted OpenSearch endpoint (via IAM permissioning and/or `AWS_*` environment variables) or using basic auth against a dev OpenSearch Docker container

### AWS Hosted OpenSearch
If you're trying to set up the record_indexer to communicate with an AWS hosted OpenSearch instance, set the `OPENSEARCH_ENDPOINT` to the AWS-provided endpoint. Make sure your machine or your AWS account has access, and, if relevant, set the following environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`.

### Dev OpenSearch Docker Container
There is also an OpenSearch dev environment docker compose file available. You can run `docker-compose up` to startup an OpenSearch instance with API access at https://localhost:9200. The default username is `admin` and the default password is `Rikolti_05`. [OpenSearch Docker Container Documentation](https://hub.docker.com/r/opensearchproject/opensearch)

Send requests to the OpenSearch REST API to verify the docker container is working. 

> By default, OpenSearch uses self-signed TLS certificates. The -k short option skips the certificate verification step so requests don't fail

```
curl -X GET "https://localhost:9200/_cat/indices" -ku admin:Rikolti_05
```

To use this docker container with the record indexer, you will have to configure: 

```
export OPENSEARCH_USER=admin
export OPENSEARCH_PASS=Rikolti_05
export OPENSEARCH_IGNORE_TLS=True
```

**To connect to this OpenSearch docker container from mwaa-local-runner, set the previous values and the below endpoint in dags/startup.sh:**

```
export OPENSEARCH_ENDPOINT=https://host.docker.internal:9200/
```

**To use this OpenSearch docker container from your host machine, set the previous values and the below endpoint in env.local:**

```
export OPENSEARCH_ENDPOINT=https://localhost:9200/
```

## Initializing an OpenSearch instance to work with Rikolti

Make sure that OPENSEARCH_ENDPOINT and the relevant authentication is set in your environment.

1. Create an index template for rikolti:

```
python -m record_indexer.index_templates.rikolti_template
```

This creates a record template that will be used for adding documents to any index with name matching `rikolti*` is added to the cluster.

2. Create an initial stage and prod indexes and add the aliases `rikolti-stg` and `rikolti-prd` to each respectively: 

```
python -m record_indexer.initialize.indices_and_aliases
```

This creates an empty index named `rikolti-<current timestamp>` (enforcing the use of the rikolti_template for all records indexed into it) and assigns it to the alias `rikolti-stg`. 

## Running the Record Indexer

TODO: We don't currently support running the indexer from the command line

## Indexer development using aws-mwaa-local-runner

See the Rikolti README page section on [Airflow Development](https://github.com/ucldc/rikolti/#airflow-development). In particular, make sure that indexer-related env vars are set as described there.

## Index lifecycle

TODO: this section is all pretty defunct now; update documentation

The lifecycle of an index is as follows:

#### Create new index
1. Create a new index named `rikolti-{collection_id}-{version}`, where `version` is the current datetime).
2. Remove any existing indices for the collection from the `rikolti-stg` alias.
3. Add the new index to the `rikolti-stg` alias.
4. Delete any older unaliased indices, retaining the number of unaliased indices specified by `settings.INDEX_RETENTION`.

Note that the index creation code enforces the existence of one stage index at a time.

#### Move staged index to production
1. Identify the current stage index for the collection.
2. Remove any existing indices for the collection from the `rikolti-prd` alias.
3. Add the current stage index to the `rikolti-prd` alias. (This means that at this stage in the lifecycle, the index will be aliased to `rikolti-stg` and `rikolti-prd` at the same time.)

#### Delete old index
This happens during index creation (see step 4. above).

## OpenSearch index record updates
An option for updating records that are already in the OpenSearch index is to use the [`update by query` API](https://opensearch.org/docs/latest/api-reference/document-apis/update-by-query/). For example, we wanted to update records in `rikolti-stg` to make the value of `type` lowercase. Here is the query that we issued to update `Text` to `text`. Since it was a quick one-off update, we simply issued this in the OpenSearch Dashboard API console:

```
POST rikolti-stg/_update_by_query
{
  "query": {
    "term": {
      "type.raw": "Text"
    }
  },
  "script": {
    "source": "ctx._source.type = ctx._source.type[0].toLowerCase()",
    "lang": "painless"
  }
}
```






