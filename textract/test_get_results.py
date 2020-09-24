# test 
from get_text import get_text

test = {
	'Records': [
		{
			'EventSource': 'aws:sns', 
			'EventVersion': '1.0', 
			'EventSubscriptionArn': 'arn:aws:sns:us-west-2:563907706919:AmazonTextractPachamama:86753363-e40a-4661-9044-87f767f43324', 'Sns': {
				'Type': 'Notification', 
				'MessageId': '60c2153a-e4ab-5306-9953-42dc000fa6b5', 
				'TopicArn': 'arn:aws:sns:us-west-2:563907706919:AmazonTextractPachamama', 
				'Subject': None, 
				'Message': '''{
					"JobId":"b9150f75d8435907ea110448f4b4078abc18de71788457525a76516ff8912aa7",
					"Status":"SUCCEEDED",
					"API":"StartDocumentTextDetection",
					"Timestamp":1600971364073,
					"DocumentLocation":{
						"S3ObjectName":"466-files/2020-09-23/466--07915626-9a72-4288-b188-f516fbe9f216::ucsf_mss2000-31_004_SFAFdinner.pdf",
						"S3Bucket":"amy-test-bucket"
					}
				}''', 
				'Timestamp': '2020-09-24T18:16:04.118Z', 
				'SignatureVersion': '1', 
				'Signature': 'iHWbpvveHfGZUidUYXq/2E6e+Ovg/iF5kv8aOztmsWV6XN+xMAoxgBf21WMCf+MQJhfOjPdpTKyR0G+HMuPEH9VOdkRC3nnsOl3oyLLVBBg8aAFndrDLVdkhw2eIeoeUwISMLNDam5uZxcyMu3DgPF/K6zuIh0Vqh1V9ZBPq4J0CkByykNEUvbeWQptwIyTGegYWKeFbCYZk36C3xI/OTF01/OtY0+fKjMkpumIjHJ/PcV2zZStk1d5bVUiKWfv+b425NCGX0X/MXUHB1kRrz14MTijDnnwZzx7tLjV4nRE/M+/sEx0qaBXPFwEJhzsgZGwknZVs/w3kXwM2WNGUfg==', 
				'SigningCertUrl': 'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem', 
				'UnsubscribeUrl': 'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:563907706919:AmazonTextractPachamama:86753363-e40a-4661-9044-87f767f43324', 'MessageAttributes': {}
			}
		}
	]
}

get_text(test, None)
