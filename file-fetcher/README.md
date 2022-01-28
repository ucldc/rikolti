# rikolti file-fetcher-docker

## Local development

### Install lando

We are using [lando](https://docs.lando.dev) for a more user-friendly way to do local development using [Docker](https://www.docker.com/). You'll need to first [install lando](https://docs.lando.dev/basics/installation.html) as per the documentation.

### Set up environment

Make a copy of the `local.env` template:

`cp local.env env.local`

Edit `env.local` with the appropriate values.

### Start lando

Simply issue the following command:

`lando start`

This will start up the app as specified in the `.lando.yml` file.

See documentation for other lando [CLI commands](https://docs.lando.dev/basics/usage.html).

### Run code

To run python code in the docker container that's been set up using lando, use the `lando python` command, e.g.:

`lando python fetch_collection_files.py`

## Deployment in AWS Fargate

We are deploying this code as a [Docker](https://www.docker.com/) container in [AWS Fargate](https://aws.amazon.com/fargate/).

The `Dockerfile` is essentially a copy of `.lando.yml`