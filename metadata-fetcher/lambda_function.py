import asyncio
import time
import json

import os
DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
    import aioboto3
    import boto3

from NuxeoFetcher import NuxeoFetcher
from OAIFetcher import OAIFetcher

instantiate = {
    'nuxeo': NuxeoFetcher,
    'oai': OAIFetcher
}

"""
AWS Lambda should have an Execution Role set to an IAM role with policy:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "lambda:InvokeFunction",
                "logs:*"
            ],
            "Resource": "<s3 bucket>"
        }
    ]
}
"""

async def timer(params):
    startTime = time.strftime('%X')
    harvest_type = params.get('harvest_type')
    try:
        if harvest_type:
            fetcher = instantiate[harvest_type](params)
            # https://docs.python.org/3/library/asyncio-task.html#timeouts
            await asyncio.wait_for(fetcher.fetch(), 120)
        else:
            print(f"bad harvest type: {params.get('harvest_type')}")
    except asyncio.TimeoutError:
        print('WHOA TIMEOUT')
        await invoke_next(await fetcher.json())

    endTime = time.strftime('%X')
    print(f"started at {startTime}")
    print(f"finished at {endTime}")

async def invoke_next(payload):
    print(f"NEW INVOCATION")
    print(payload)
    if (DEBUG):
        await timer(json.loads(payload))
    else:
        # time to spawn a new lambda
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html
        lambda_client = boto3.client('lambda', region_name="us-west-2",)
        lambda_client.invoke(
          FunctionName="async-fetch",
          InvocationType="Event", #invoke asynchronously
          Payload=payload.encode('utf-8')
        )

        # async doesn't seem to work
        # async with aioboto3.resource('lambda', region_name='us-west-2') as lambda_client:
        #     await lambda_client.invoke(
        #         FunctionName="async-fetch",
        #         InvocationType="Event", #invoke asynchronously
        #         Payload=await json.dumps(payload).encode('utf-8')
        #     )

def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)
    asyncio.run(timer(payload))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
