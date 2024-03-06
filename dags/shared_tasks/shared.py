import boto3
import os
import json
import traceback

import requests

from airflow.decorators import task

from urllib.parse import urlparse

def send_event_to_sns(context: dict, task_message: dict):
    """
    Send a log message to an SNS topic for a specific job.

    Args:
        context (dict): Airflow's context dictionary:
            https://docs.astronomer.io/learn/airflow-context
            https://airflow.apache.org/docs/apache-airflow/2.0.0/concepts.html#accessing-current-context
            https://composed.blog/airflow/execute-context
        task_message (dict): Dictionary containing the task's specific message
    
    Returns:
        None
    """
    dag_run = context['dag_run']
    log_message = {
        'dag_id': dag_run.dag_id,
        'dag_run_id': dag_run.run_id,
        'logical_date': dag_run.logical_date.isoformat(),
        'dag_run_conf': dag_run.conf,
    }
    task_instance = context.get('task_instance')
    if task_instance:
        log_message.update({
            'task_id': task_instance.task_id,
            'try_number': task_instance.try_number,
            'map_index': task_instance.map_index,
            # TODO: this doesn't actually work to get the task parameters
            # 'task_params': task_instance.xcom_pull(task_ids=task_instance.task_id),
        })
    log_message['rikolti_message'] = task_message
    message_body = json.dumps(log_message)

    sns = boto3.client('sns')
    topic_arn = os.environ.get('RIKOLTI_EVENTS_SNS_TOPIC', '')
    try:
        response = sns.publish(
            TopicArn=topic_arn,
            Message=message_body
        )
        print(f"Message sent to SNS with Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send message to SQS: {e}")


def notify_rikolti_failure(context):
    exception = context['exception']
    tb_as_str_list = traceback.format_exception(
        type(exception), exception, exception.__traceback__)
    exc_as_str_list = traceback.format_exception_only(
        type(exception), exception)

    rikolti_message = {
        'error': True,
        'exception': '\n'.join(exc_as_str_list),
        'traceback': ''.join(tb_as_str_list)
    }
    send_event_to_sns(context, rikolti_message)


def notify_dag_success(context):
    rikolti_message = {'dag_complete': True}
    send_event_to_sns(context, rikolti_message)


def notify_dag_failure(context):
    rikolti_message = {
        'dag_complete': False,
        'error': True,
        'reason': context.get('reason', 'Unknown reason')
    }
    send_event_to_sns(context, rikolti_message)


@task(multiple_outputs=True, 
      task_id="get_registry_data", 
      on_failure_callback=notify_rikolti_failure)
def get_registry_data_task(params=None, **context):
    if not params or not params.get('collection_id'):
        raise ValueError("Collection ID not found in params")
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikolticollection/{collection_id}/?format=json"
    )
    resp.raise_for_status()
    registry_data = resp.json()

    # TODO: remove the rikoltifetcher registry endpoint and restructure
    # the fetch_collection function to accept a rikolticollection resource.
    fetchdata_resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikoltifetcher/{collection_id}/?format=json"
    )
    fetchdata_resp.raise_for_status()
    registry_data['registry_fetchdata'] = fetchdata_resp.json()

    send_event_to_sns(context, registry_data)

    return registry_data


# TODO: in python3.12 we can use itertools.batched
def batched(list_to_batch, batch_size):
    batches = []
    for i in range(0, len(list_to_batch), batch_size):
        batches.append(list_to_batch[i:i+batch_size])
    return batches


@task(task_id="make_registry_endpoint")
def make_registry_endpoint_task(params=None):
    if not params:
        raise ValueError("No parameters provided")

    arg_keys = ['mapper_type', 'rikolti_mapper_type', 'registry_api_queryset']
    args = {key: params.get(key) for key in arg_keys if params.get(key)}
    if not any(args.values()):
        raise ValueError("Endpoint data not found in params, please provide "
                         "either a mapper_type, a rikolti_mapper_type, or a "
                         "registry_api_queryset")

    which_arg = list(args.keys())
    if len(which_arg) > 1:
        raise ValueError("Please provide only one of mapper_type, "
                         "rikolti_mapper_type, or registry_api_queryset")

    which_arg = which_arg[0]
    if which_arg == 'mapper_type':
        mapper_type = params.get('mapper_type')
        endpoint = (
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json"
            f"&mapper_type={mapper_type}&ready_for_publication=true"
        )
    elif which_arg == 'rikolti_mapper_type':
        rikolti_mapper_type = params.get('rikolti_mapper_type')
        endpoint = (
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json"
            f"&rikolti_mapper_type={rikolti_mapper_type}"
            "&ready_for_publication=true"
        )
    elif which_arg == 'registry_api_queryset':
        endpoint = params.get('registry_api_queryset')
        # TODO: validate endpoint is a valid registry endpoint describing
        # a queryset of collections
    else:
        raise ValueError(
            "Please provide a mapper_type, rikolti_mapper_type, or endpoint")

    offset = params.get('offset')
    if offset:
        endpoint = endpoint + f"&offset={offset}"

    print("Fetching, mapping, and validating collections listed at: ")
    print(endpoint)
    return endpoint


@task()
def s3_to_localfilesystem(s3_url=None, params=None):
    """
    Download all files at a specified s3 location to the local filesystem.
    Requires an s3_url specified as an argument, or an s3_url in the DAG run
    parameters. The s3_url should be specified in s3://<bucket>/<prefix>
    format.
    """
    if not s3_url:
        s3_url = params.get('s3_url', None) if params else None
        if not s3_url:
            raise ValueError("No s3_url specified in params or as argument")

    # parse s3_url
    s3_url = urlparse(s3_url)
    if s3_url.scheme != 's3':
        raise ValueError(
            "s3_url must be specified in s3://<bucket>/<prefix> format")
    bucket = s3_url.netloc
    path = s3_url.path[1:]

    # query s3 for a list of files filtered by path
    s3_client = boto3.client('s3')
    keys = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
    if keys['KeyCount'] == 0:
        raise ValueError(f"No files found at {s3_url}")

    paths = []
    for key in keys['Contents']:
        # get the contents of a single s3 file
        obj = s3_client.get_object(Bucket=bucket, Key=key['Key'])
        contents = obj['Body'].read().decode('utf-8')

        # create directory structure represented by s3 path in local filesystem
        path = key['Key'].split('/')
        path.insert(0, 'tmp')
        path = os.path.sep + os.path.sep.join(path)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        # write contents of s3 file to local filesystem
        with open(path, 'wb') as sync_file:
            sync_file.write(contents.encode('utf-8'))
        paths.append(path)

    return paths


