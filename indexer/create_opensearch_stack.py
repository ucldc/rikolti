import sys
import argparse
import boto3
import botocore
import create_template_opensearch
from datetime import datetime
import pprint
pp = pprint.PrettyPrinter(indent=4)

'''
Create AWS CloudFormation stack for Rikolti OpenSearch index
'''

cf_client = boto3.client('cloudformation')

def main(params):
    datetime_string = datetime.today().strftime('%Y%m%d-%H%M')
    stack_name = f"rikolti-opensearch-{params.env}-{datetime_string}"

    template_file = create_template_opensearch.main()

    with open(template_file) as f:
        template_body = f.read()
    
    cf_client.validate_template(TemplateBody=template_body)

    parameters = [
        {
            "ParameterKey": "Env",
            "ParameterValue": params.env
        }
    ]

    tags = [
        {
            "Key": "Program",
            "Value": "dsc"
        },
        {
            "Key": "Service",
            "Value": "rikolti"
        },
        {
            "Key": "Env",
            "Value": params.env
        },
    ]

    if params.env == 'prod':
        termination_protection = True
    else:
        termination_protection = False

    create_stack_params = {
        'StackName': stack_name,
        'TemplateBody': template_body,
        'Parameters': parameters,
        'DisableRollback': params.disable_rollback,
        'TimeoutInMinutes': 60, # takes a long time!
        #'Capabilities': ['CAPABILITY_NAMED_IAM'],
        'Tags': tags,
        'EnableTerminationProtection': termination_protection
    }

    try:
        print (f"Creating stack `{stack_name}`")
        response = cf_client.create_stack(**create_stack_params)
        waiter = cf_client.get_waiter('stack_create_complete')

        print("Waiting for stack to be ready...")
        waiter.wait(StackName=stack_name)
    except botocore.exceptions.ClientError as e:
        raise(e)
    else:
        pp.pprint(cf_client.describe_stacks(StackName=response['StackId']))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('env', choices=['prod', 'stg', 'dev'])
    parser.add_argument('--disable_rollback', help='disable rollback', action=argparse.BooleanOptionalAction, default=False),
    args = parser.parse_args()
    sys.exit(main(args))