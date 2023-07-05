# Airflow Development

## Set up `aws-mwaa-local-runner`

AWS provides the [aws-mwaa-local-runner](https://github.com/aws/aws-mwaa-local-runner) repo, which provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally via use of a Docker container. We have forked this repository and made some small changes to enable us to use local-runner while keeping our dags stored in this repository. (See this slack thread for more info: https://apache-airflow.slack.com/archives/CCRR5EBA7/p1690405849653759)

To set up this dev environment, first clone the repo locally:

```
git clone git@github.com:ucldc/aws-mwaa-local-runner.git
```

Then, modify `aws-mwaa-local-runner/docker/config/.env.localrunner`, setting the following env vars to wherever the directories live on your machine, for example:

```
DAGS_HOME="/Users/username/dev/rikolti/airflow/dags"
PLUGINS_HOME="/Users/username/dev/rikolti/airflow/plugins"
REQS_HOME="/Users/username/dev/rikolti/airflow"
STARTUP_HOME="/Users/username/dev/rikolti/airflow"
```

These env vars are used in the `aws-mwaa-local-runner/docker/docker-compose-local.yml` script (and other docker-compose scripts) to mount the relevant directories containing Airflow DAGs, requirements, and plugins files into the docker container.

Then, follow the instructions in the [README](https://github.com/ucldc/aws-mwaa-local-runner/#readme) to build the docker image, run the container, and do local development.


