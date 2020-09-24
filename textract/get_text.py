import boto3
import json
import os
import time


def get_text(sns_message, context):
	textract_client = boto3.client('textract')
	s3_client = boto3.client('s3')
	
	job=json.loads(sns_message['Records'][0]['Sns']['Message'])
	date=sns_message['Records'][0]['Sns']['Timestamp'].split('T')[0]

	print(job['Status'])
	if job['Status'] != 'SUCCEEDED':
		print((
			f"{job['Status']}: s3://"
			f"{job['DocumentLocation']['S3Bucket']}"
			f"{job['DocumentLocation'][S3ObjectName]}"
		))
		return

	next=True
	page=0
	words = []
	calisphere_id = job['DocumentLocation']['S3ObjectName'].split('/')[-1].split('::')[0]
	while(next):
		if page==0:
			response = textract_client.get_document_text_detection(
				JobId=job['JobId'])
		else:
			response = textract_client.get_document_text_detection(
				JobId=job['JobId'],
				NextToken=next)


		blocks = response.get('Blocks')
		for block in blocks:
			if block['BlockType'] == "LINE" and block['Confidence'] > 50:
				words.append(block['Text'])

		next = response.get('NextToken')

		s3_client.put_object(
			Bucket='amy-test-bucket',
			Body=json.dumps(response).encode('utf-8'),
			Key=f"466-textract/{date}/textract-results/{calisphere_id}/{page}.json"
		)
		page+=1

	textract_record = s3_client.get_object(
		Bucket='amy-test-bucket',
		Key=f"466-textract/{date}/{calisphere_id}.json"
	)
	job_record = json.loads(textract_record['Body'].read())
	job_record['word_bucket'] = " ".join(words)
	s3_client.put_object(
		Bucket='amy-test-bucket',
		Key=f"466-textract/{date}/{calisphere_id}.json",
		Body=json.dumps(job_record).encode('utf-8')
	)
	
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}


