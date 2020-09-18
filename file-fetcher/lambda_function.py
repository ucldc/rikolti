import asyncio
import time
import json

# TODO: Diff with lambda-fetcher/lambda_function.py 
# for the very small differences; refactor for reuse

import os
DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
    import aioboto3
    import boto3

from file_fetcher import FileFetcher

instantiate = {
    'nuxeo': FileFetcher,
}

# https://docs.python.org/3/library/asyncio-task.html#timeouts
async def timer(params):
    startTime = time.strftime('%X')
    print(params)
    harvest_type = params.get('harvest_type')
    try:
        if harvest_type:
            file_fetcher = instantiate[harvest_type](params)
            await asyncio.wait_for(file_fetcher.fetch_content_files(), 600)
        else:
            print(f"bad harvest type: {params.get('harvest_type')}")
    except asyncio.TimeoutError:
        print('WHOA TIMEOUT')
        await invoke_next(await file_fetcher.json())

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
          FunctionName="async-file-fetch",
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
