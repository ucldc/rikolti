import boto3
import json
import time
import botocore

def lambda_handler(payload, context):
	collection_id = payload.get('collection_id')
	file_fetch_date = payload.get('file_fetch_date')

	s3_client = boto3.client('s3')
	textract_client = boto3.client('textract')

	s3_list_objects = s3_client.list_objects_v2(
		Bucket="amy-test-bucket", 
		Prefix=f"{collection_id}-files/{file_fetch_date}")
	keys = [file['Key'] for file in s3_list_objects['Contents']]

	start_line = payload.get('start_line', 0)
	number_run = payload.get('number_run', len(keys))

	for key in keys[start_line:start_line+number_run]:
		try:
			textract_job = textract_client.start_document_text_detection(
				DocumentLocation={
					'S3Object': {
						'Bucket': 'amy-test-bucket',
						'Name': key,
					}
				},
				NotificationChannel={
					'SNSTopicArn': "arn:aws:sns:us-west-2:563907706919:AmazonTextractPachamama",
					'RoleArn': "arn:aws:iam::563907706919:role/TextractRole"
				}
			)
			print({'calisphere-id': key, 'job_id': textract_job})
		except botocore.exceptions.ClientError as e:
			print(e)
			break

		calisphere_id = key.split('/')[-1].split('::')[0]
		content_filename = key.split('/')[-1].split('::')[1]
		textract_recordname = (
			f"{collection_id}-textract/"
			f"{time.strftime('%Y-%m-%d')}/"
			f"{calisphere_id}.json"
		)
		body = {
			'calisphere-id': calisphere_id,
			'analyzed_filename': content_filename,
			'textract_job': textract_job
		}
		response = s3_client.put_object(
			ACL='bucket-owner-full-control',
			Bucket='amy-test-bucket',
			Body=json.dumps(body).encode('utf-8'),
			Key=textract_recordname
		)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}