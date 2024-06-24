# Commands to run on the host machine for use with the opensearch-node container
# you can run with a docker-compose up in this same directory

printf "\n\n>>> Creating the 'rikolti-dev-index' index\n"
curl -X PUT "https://localhost:9200/rikolti-dev-index" -ku admin:Rikolti_05

printf "\n\n>>> Creating the 'rikolti' template\n"
export RIKOLTI_ES_ENDPOINT=https://localhost:9200/
export RIKOLTI_ES_USER=admin
export RIKOLTI_ES_PASS="Rikolti_05"
export RIKOLTI_ES_IGNORE_TLS=True
python -m record_indexer.index_templates.rikolti_template

printf "\n>>> Creating the 'rikolti-stg' alias\n"
curl -X POST -H "Content-Type:application/json" "https://localhost:9200/_aliases" -ku admin:Rikolti_05 -d '{"actions": [{"add": {"index": "rikolti-dev-index", "alias": "rikolti-stg"}}]}'

printf "\n\n>>> Verify the 'rikolti-dev-index' index\n"
curl -X GET "https://localhost:9200/_cat/indices" -ku admin:Rikolti_05

printf "\n\n>>> Verify the 'rikolti-stg' alias\n"
curl -X GET "https://localhost:9200/_cat/aliases" -ku admin:Rikolti_05
