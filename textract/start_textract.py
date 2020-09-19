import boto3
import json
import os
import time
import botocore

DEBUG = os.environ['DEBUG']
AMZ_ID = os.environ['AMZ_ID']
AMZ_SECRET = os.environ['AMZ_SECRET']

def lambda_handler(payload, context):
	collection_id = payload.get('collection_id')
	file_fetch_date = payload.get('file_fetch_date')

	s3_client = boto3.client('s3', aws_access_key_id=AMZ_ID, aws_secret_access_key=AMZ_SECRET)
	textract_client = boto3.client('textract', aws_access_key_id=AMZ_ID, aws_secret_access_key=AMZ_SECRET)

	s3_list_objects = s3_client.list_objects_v2(
		Bucket="amy-test-bucket", 
		Prefix=f"{collection_id}-files/{file_fetch_date}")
	keys = [file['Key'] for file in s3_list_objects['Contents']]

	for key in keys:
		try:
			textract_job = textract_client.start_document_text_detection(
				DocumentLocation={
					'S3Object': {
						'Bucket': 'amy-test-bucket',
						'Name': key,
					}
				},
				NotificationChannel={
					'SNSTopicArn': "arn:aws:sns:us-west-2:563907706919:textract-jobs",
					'RoleArn': ""
				}
			) 
			print({'calisphere-id': key, 'job_id': textract_job})
		except botocore.exceptions.ClientError as e:
			print(e)
			break

		response = s3_client.put_object(
			Bucket='amy-test-bucket',
			Body=json.dumps({'calisphere-id': key, 'job_id': textract_job}).encode('utf-8'),
			Key=f"{collection_id}-textract/{time.strftime('%Y-%m-%d')}/{key.split('/')[-1].split('.')[0]}.json"
		)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}

print(time.strftime('%X'))
lambda_handler({'collection_id': 466, 'file_fetch_date': '2020-09-18'}, {})
print(time.strftime('%X'))