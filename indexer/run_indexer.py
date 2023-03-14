# CREATE CANDIDATE INDEX
# Use clone API to make a clone of the `prd` index, making sure that read_only mode is NOT set.
# Alternatively, create a new empty index
# Update `stg` alias to point to newly created index.


# COLLECTION/BULK UPDATE
# use bulk API to delete any existing records
# use bulk API to index all json records on S3

# SINGLE ITEM UPDATE
# For deletions: issue a DELETE request on the document
# For updates: create a json document containing the updated fields and issue an update/upsert request with it as the payload
# For new objects: create a json document representing the mapped data and issue an index request with it as the payload

# set calisphere_id as document _id - needs to be passed in explicitly

'''
index_name = 'test'
# delete index if it exists - for testing!!
r = requests.delete(url, auth=auth)
r.raise_for_status()
'''

# do we have to load into memory before using bulk API?