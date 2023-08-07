import boto3
import json
import botocore

def lambda_handler(payload, context):
	collection_id = payload.get('collection_id')

	s3_client = boto3.client('s3')
	textract_client = boto3.client('textract')

	s3_list_objects = s3_client.list_objects_v2(
		Bucket="rikolti-public",
		Prefix=f"content_files/{collection_id}/")
	keys = [file['Key'] for file in s3_list_objects['Contents']]
	keys = [k for k in keys if k.split('.')[-1] == 'pdf']

	start_line = payload.get('start_line', 0)
	number_run = payload.get('number_run', len(keys))

	for key in keys[start_line:start_line+number_run]:
		try:
			calisphere_id = key.split('/')[-1].split('::')[0]
			textract_job = textract_client.start_document_text_detection(
				DocumentLocation={
					'S3Object': {
						'Bucket': 'rikolti-public',
						'Name': key,
					}
				},
				NotificationChannel={
					'SNSTopicArn': "arn:aws:sns:us-west-2:563907706919:AmazonTextractPachamama",	# noqa: E501
					'RoleArn': "arn:aws:iam::563907706919:role/TextractRole"
				},
				OutputConfig={
					"S3Bucket": "rikolti",
					"S3Prefix": f"textract/raw-output/{collection_id}/{calisphere_id}/textract-raw"	# noqa: E501
				}
			)
			print({'calisphere-id': key, 'job_id': textract_job})
		except botocore.exceptions.ClientError as e:
			print(e)
			break

		calisphere_id = key.split('/')[-1].split('::')[0]
		content_filename = key.split('/')[-1].split('::')[1]
		textract_recordname = (
			f"textract/{collection_id}/"
			f"{calisphere_id}.json"
		)
		body = {
			'calisphere-id': calisphere_id,
			'analyzed_filename': content_filename,
			'textract_job': textract_job
		}
		response = s3_client.put_object(
			ACL='bucket-owner-full-control',
			Bucket='rikolti',
			Body=json.dumps(body).encode('utf-8'),
			Key=textract_recordname
		)
		print(response)

	return {
		'statusCode': 200,
		'body': json.dumps(f'Started Textract for {collection_id}')
	}