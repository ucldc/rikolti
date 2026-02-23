---
name: "[template] Metadata Edit"
about: Updating one-off Calisphere metadata records
title: 'Metadata update for # item record [#Reg ID]'
labels: ''
assignees: ''

---

## Useful links to reference
Freshdesk: 
Harvesting issue: 
Item record URL: 

## Metadata edits
Currently Published Metadata Version (from Rikolti-data):
ZIP upload (edited json): 

## Steps
- [ ] Metadata edit request received from contributor
- [ ] Find the published version of the metadata for the collection (see the ops manual for details)
- [ ] Download all json files from `rikolti-data`
- [ ] Edit json files, as needed
- [ ] Zip all json files, and upload to this issue
- [ ] Amy adds the candidate metadata's S3 URL & version path (with `<today>`)
- [ ] Run the `index_manual_edits` DAG in Airflow; review on -stage
- [ ] Run the `publish` DAG in Registry; review on -prod
