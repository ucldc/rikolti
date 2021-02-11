import os
import json
import requests
from requests.auth import HTTPBasicAuth

ES_HOST = os.environ['ES_HOST']
ES_USER = os.environ['ES_USER']
ES_PASS = os.environ['ES_PASS']

class NuxeoESIndexer(object):

    ''' 
            takes a jsonl string and adds record to ES index 

            TODO: I think we want to batch index, but since 
            these records are coming in one per object as
            jsonl, let's just do it this way for the demo
    '''

    def __init__(self, content):

        self.content = content
        self.document = self.create_document(self.content) 

    def create_document(self, json_content):

        ''' prep the data as necessary '''
        return json_content 

    def index_document(self, document, doc_id, index):
        
        host = ES_HOST
        user = ES_USER
        passwd = ES_PASS

        url = f'{host}/{index}/_doc/{doc_id}'
        headers = { "Content-Type": "application/json" }

        document = json.loads(document)

        r = requests.put(url, auth=HTTPBasicAuth(user, passwd), json=document, headers=headers)
        print(r.text)
