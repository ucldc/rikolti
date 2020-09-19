import boto3
import json
import os
import time
import botocore

DEBUG = os.environ['DEBUG']
AMZ_ID = os.environ['AMZ_ID']
AMZ_SECRET = os.environ['AMZ_SECRET']

def lambda_handler(payload, context):
	textract_client = boto3.client('textract', aws_access_key_id=AMZ_ID, aws_secret_access_key=AMZ_SECRET)
	
	jobs_file = open("textract-jobs.jsonl", "r")
	for line in jobs_file:
		job = json.loads(line)

		next=True
		page=0
		while(next):
			if page==0:
				response = textract_client.get_document_text_detection(
					JobId=job['job_id']['JobId'])
			else:
				response = textract_client.get_document_text_detection(
					JobId=job['job_id']['JobId'],
					NextToken=next)
			wf = open(f"./textract-results/{job['calisphere-id'].split('.')[0].split('/')[-1]}/{page}.json", "w+")
			wf.write(json.dumps(response))
			wf.close()
			page+=1
			next=response.get('NextToken')




lambda_handler(None, None)

