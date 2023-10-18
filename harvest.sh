#!/bin/bash

export RIKOLTI_HOME="/Users/awieliczka/Projects/rikolti"
source $RIKOLTI_HOME/env.local
source ~/.envs/rikolti/bin/activate
which python

python -m metadata_fetcher.fetch_registry_collections \
    "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=$1&ready_for_publication=true" \
    > $RIKOLTI_HOME/rikolti_data/$1_fetcher_job.csv
echo "fetcher job finished"

python -m metadata_mapper.map_registry_collections \
    "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json&mapper_type=$1&ready_for_publication=true" \
    > $RIKOLTI_HOME/rikolti_data/$1_mapper_job.csv
echo "mapper job finished"

ls -d $RIKOLTI_HOME/rikolti_data/*/ | \
    while read FOLDER; do \
        COLLECTION=$(basename $FOLDER); \
        echo "processing $COLLECTION"; \
        python -m metadata_mapper.validate_mapping $COLLECTION \
            > "$RIKOLTI_HOME/rikolti_data/$COLLECTION/validation_job.txt"; \
    done
