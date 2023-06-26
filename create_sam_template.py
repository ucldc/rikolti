import sys
from troposphere import Parameter, Ref, Sub, Template #, Output
from troposphere import GetAtt
import troposphere.serverless as serverless
import troposphere.awslambda as awslambda
import troposphere.iam as iam
from awacs.aws import Action, Allow, PolicyDocument, Principal, Statement

'''
    THIS SCRIPT IS A STUB! We are not currently using this troposphere script
    as it currently provides no advantage over just writing the template.yaml
    file by hand. Keeping it around in case it becomes useful in the future,
    e.g. for generating templates for different environment.
'''
def main():

    template = Template()

    template.set_description(
        "SAM Template for Rikolti metadata_fetcher and metadata_mapper")
    template.set_transform("AWS::Serverless-2016-10-31")
    template.set_version("2010-09-09")
    template.set_globals({"function": {"Timeout": 900}})

    ####################
    ## PARAMETERS
    ####################
    nuxeo_api_token = template.add_parameter(Parameter(
        "NuxeoApiToken",
        Description="Token for Nuxeo API",
        Type="String",
        # TODO: set this up in secrets manager
        #Default="{{resolve:secretsmanager:nuxeo/api_token:SecretString:token}}"
        Default = "abc-def-ghi"
    ))

    ####################
    ## IAM ROLE RESOURCE
    ####################
    role_logical_id = "MetadataFetcherFunctionRole"

    fetcher_function_role = template.add_resource(iam.Role(
        role_logical_id,
        RoleName = Sub("${AWS::StackName}-" + role_logical_id)
    ))

    assume_role_policy_document = PolicyDocument(
        Version="2012-10-17",
        Id=f"{role_logical_id}-AssumeRole-Permissions",
        Statement=[
            Statement(
                Effect=Allow,
                Principal=Principal("Service", ["lambda.amazonaws.com"]),
                Action=[Action("sts", "AssumeRole")]
            )
        ]
    )

    fetcher_function_role.AssumeRolePolicyDocument = assume_role_policy_document

    ######################
    ## IAM POLICY RESOURCE
    ######################
    fetcher_policy = template.add_resource(iam.PolicyType(
        "MetadataFetcherPolicy",
        PolicyName = Sub("${AWS::StackName}-metadata-fetcher-policy")
    ))

    policy_document = PolicyDocument(
        Version="2012-10-17",
        Id=f"{role_logical_id}-Permissions",
        Statement=[
            Statement(
                Effect=Allow,
                Action=[
                    Action("s3", "PutObject"),
                    Action("s3", "GetObject"),
                    Action("s3", "ListBucket"),
                    Action("s3", "PutObjectAcl"),
                    Action("logs", "*"),
                    Action("lambda", "InvokeFunction")
                ],
                Resource=["*"],
            ),
        ],
    )

    fetcher_policy.PolicyDocument = policy_document

    # associate role with policy
    fetcher_policy.Roles = [
        Ref(role_logical_id)
    ]

    #################################
    ## LAMBDA FUNCTION fetch_metadata
    #################################
    metadata_fetcher_function = template.add_resource(serverless.Function(
        "MetadataFetcherFunction",
        FunctionName = "fetch_metadata",
        CodeUri = "metadata_fetcher/",
        Handler = "lambda_function.fetch_collection",
        Runtime = "python3.9",
        Architectures = ['x86_64'],
        Role = GetAtt(fetcher_function_role, "Arn")
    ))

    metadata_fetcher_function.Environment = awslambda.Environment(
        Variables = {
            "NUXEO": Ref(nuxeo_api_token),
            "S3_BUCKET": "rikolti"
        }
    )

    ###################################
    ## LAMBDA FUNCTION shepherd_mappers
    ###################################
    shepherd_mappers_function = template.add_resource(serverless.Function(
        "MetadataMapperShepherdFunction",
        FunctionName = "shepherd_mappers",
        CodeUri = "metadata_mapper/",
        Handler = "lambda_shepherd.map_collection",
        Runtime = "python3.9",
        Architectures = ['x86_64']
        # Role = GetAtt(shepherd_mappers_function_role, "Arn")
    ))

    shepherd_mappers_function.Environment = awslambda.Environment(
        Variables = {
            "SKIP_UNDEFINED_ENRICHMENTS": True
        }
    )

    ###################################
    ## LAMBDA FUNCTION map_metadata
    ###################################
    # map_metadata_function = template.add_resource(serverless.Function(
    #     "MetadataMapperMapPageFunction",
    #     FunctionName = "map_metadata",
    #     CodeUri = "metadata_mapper/",
    #     Handler = "lambda_function.map_page",
    #     Runtime = "python3.9",
    #     Architectures = ['x86_64']
    #     # Role = GetAtt(shepherd_mappers_function_role, "Arn")
    # ))

    shepherd_mappers_function.Environment = awslambda.Environment(
        Variables = {
            "SKIP_UNDEFINED_ENRICHMENTS": True
        }
    )

    with open("template.yaml", 'w') as f:
        f.write(template.to_yaml())

if __name__ == "__main__":
    sys.exit(main())