#!/bin/bash

export RIKOLTI_HOME="/Users/awieliczka/Projects/rikolti"
source $RIKOLTI_HOME/env.local
which python

python -m metadata_fetcher.fetch_registry_collections \
    "https://registry.cdlib.org/api/v1/rikoltifetcher/$1?format=json" \
    > $RIKOLTI_HOME/rikolti_data/$1_fetcher_job.txt
echo "fetcher job finished"

python -m metadata_mapper.map_registry_collections \
    "https://registry.cdlib.org/api/v1/rikoltifetcher/$1?format=json" \
    > $RIKOLTI_HOME/rikolti_data/$1_mapper_job.txt
echo "mapper job finished"

echo "processing $1"
python validate_mapping.py $1 \
    > $RIKOLTI_HOME/rikolti_data/$1/validation_job.txt
