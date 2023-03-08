docker build -t content_harvester .
docker compose run --rm content_harvester https://registry.cdlib.org/api/v1/rikoltimapper/26147/?format=json

--rm flag removes the container after run.

default entrypoint is `by_registry_endpoint.py` 

requires an env.local adjacent to the docker-compose in order to run (check settings.py for hints on what needs to be defined in env.local)

TODO:
- write records to s3 individually rather than in batches as pages (so that if a content harvest fails, we can pick up again from point of failure)
- md5 the thumbnails
- change folder name "mapped_with_content"
- tune log output (this module is v. noisy currently)
- add error handling
- join child metadata records to parent records?
- modify mappers to use thumbnail source format that the content harvester expects
- figure out AWS deployment (ECR, Fargate, ECS)