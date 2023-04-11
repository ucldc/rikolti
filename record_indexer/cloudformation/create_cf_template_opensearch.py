import sys
from troposphere import Parameter, Output, Ref, Sub, Template, Tags
import troposphere.opensearchservice as opensearch
import troposphere.ec2 as ec2
from awacs.aws import Action, Allow, PolicyDocument, Principal, Statement
import json

def main():
    template = Template()

    ####################
    ## PARAMETERS
    ####################
    env = template.add_parameter(Parameter(
        "Env",
        Description="Environment",
        Type="String"
    ))

    '''
    vpc_id = template.add_parameter(Parameter(
        "VpcId",
        Description = "VPC Id",
        Type = "String"
    ))
    '''
    '''
    calisphere_app_sg = template.add_parameter(Parameter(
        "CalisphereAppSecurityGroup",
        Description = "Calisphere application server security group",
        Type = "String"
    ))
    '''

    ##############################
    ## SECURITY GROUP
    ##############################
    # using fine-grained access control with master user instead of security group for now
    '''
    security_group = template.add_resource(ec2.SecurityGroup(
        "SecurityGroup",
        GroupDescription = "Security Group for Rikolti OpenSearch domains",
        GroupName = "rikolti-opensearch",
        #VpcId = Ref(vpc_id),
        SecurityGroupIngress = [
            #ec2.SecurityGroupRule(
            #    Description = "Allow connections from calisphere app servers",
            #    IpProtocol = "tcp",
            #    FromPort = "0",
            #    ToPort = "65535",
            #    SourceSecurityGroupId = Ref(calisphere_app_sg)
            #),
            ec2.SecurityGroupRule(
                Description = "Allow ssh access from blackstar",
                IpProtocol = "tcp",
                FromPort = "22",
                ToPort = "22",
                CidrIp = "10.60.1.64/32"
            )
        ]
    ))
    '''

    ##############################
    ## OPENSEARCH DOMAIN
    ##############################
    '''
    Provisioned IOPS 3000 IOPS
    Provisioned Throughput (MiB/s) 125 MiB/s
    '''
    '''
    {"error":{"root_cause":[{"type":"security_exception","reason":"no permissions for [indices:monitor/stats] and User [name=arn:aws:iam::563907706919:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_pad-dsc-admin_7703dd36bc6fc585, backend_roles=[arn:aws:iam::563907706919:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_pad-dsc-admin_7703dd36bc6fc585], requestedTenant=null]"}],"type":"security_exception","reason":"no permissions for [indices:monitor/stats] and User [name=arn:aws:iam::563907706919:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_pad-dsc-admin_7703dd36bc6fc585, backend_roles=[arn:aws:iam::563907706919:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_pad-dsc-admin_7703dd36bc6fc585], requestedTenant=null]"},"status":403}
    '''
    domain = template.add_resource(opensearch.Domain(
        "OpenSearchDomain",
        # TODO: Add access policy
        # grant permissions for indices monitor/stats
        #AccessPolicies = {},
        AdvancedOptions = {
            "rest.action.multi.allow_explicit_index": "true",
            "indices.fielddata.cache.size": 20,
            "indices.query.bool.max_clause_count": "1024",
            #"override_main_response_version": "true"
        },
        #DomainEndpointOptions = DomainEndpointOptions,
        # TODO: build domain name
        DomainName = Sub("rikolti-${Env}-opensearch"),
        # Add encryption settings
        # Required HTTPS yes
        # Node-to-node encryption Yes
        # Encryption at rest Yes
        # AWS KMS Key
        EncryptionAtRestOptions = opensearch.EncryptionAtRestOptions(
            Enabled = True
        ),
        EBSOptions = opensearch.EBSOptions(
            EBSEnabled = True,
            VolumeSize = 10,
            VolumeType = 'gp3' # gp2 | gp3 | io1 | standard
        ),
        EngineVersion = 'OpenSearch_2.3',
        #LogPublishingOptions = {},
        #Tags = Tags,
        #VPCOptions = opensearch.VPCOptions(
        #    SecurityGroupIds = [],
        #    SubnetIds = []
        #)

    ))

    # Access Policy
    '''
    policy_document = PolicyDocument(
        Version="2012-10-17",
        Id=f"OpenSearchDomainPermissions",
        Statement=[
            Statement(
                Effect = Allow,
                Principal = Principal("AWS", "*"),
                Action = [
                    Action("es", "*")
                ],
                Resource=[
                    Sub("${GettAtt(domain, 'Arn')}/*")
                ]
            )
        ],
    )

    domain.AccessPolicies = policy_document
    '''
    # FIXME this causes a circular dependency error:
    access_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "*"
                },
                "Action": "es:*",
                "Resource": Sub("${OpenSearchDomain.Arn}/*")
            }
        ]
    }

    domain.AccessPolicies = access_policy


    # TODO: figure out whether to use dedicated master nodes,
    # warm nodes, zone awareness (only if you have replica shards)
    domain.ClusterConfig = opensearch.ClusterConfig(
        #DedicatedMasterCount = 3,
        #DedicatedMasterEnabled = True,
        #DedicatedMasterType = 'm3.medium.search',
        InstanceCount = 3, # data nodes
        InstanceType = 't3.medium.search', # data nodes
        #WarmCount = 1,
        #WarmEnabled = True,
        #WarmType = '',
        #ZoneAwarenessConfig = ZoneAwarenessConfig,
        #ZoneAwarenessEnabled = True
    )

    # TODO: add master user `rikolti` with password
    # https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html
    domain.AdvancedSecurityOptions = opensearch.AdvancedSecurityOptionsInput(
        Enabled = True,
        InternalUserDatabaseEnabled = True,
        MasterUserOptions = opensearch.MasterUserOptions(
            #MasterUserARN = '',
            MasterUserName = '',
            MasterUserPassword = ''
         )
    )

    filepath = 'cf_opensearch.yaml'
    with open(filepath, 'w') as f:
        f.write(template.to_yaml())

    print(f"Wrote CloudFormation template file to `{filepath}`")

    return filepath

if __name__ == "__main__":
    sys.exit(main())