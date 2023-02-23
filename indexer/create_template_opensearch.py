import sys
from troposphere import Parameter, Output, Ref, Sub, Template, Tags
import troposphere.opensearchservice as opensearch
import troposphere.ec2 as ec2

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
    # using fine-grained access control with master user for now
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
    domain = template.add_resource(opensearch.Domain(
        "OpenSearchDomain",
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
        EncryptionAtRestOptions = opensearch.EncryptionAtRestOptions(
            Enabled = True
        )
        EBSOptions = opensearch.EBSOptions(
            EBSEnabled = True,
            VolumeSize = 10,
            VolumeType = 'gp2' # gp2 | gp3 | io1 | standard
        ),
        EngineVersion = 'OpenSearch_2.3',
        #LogPublishingOptions = {},
        #Tags = Tags,
        #VPCOptions = opensearch.VPCOptions(
        #    SecurityGroupIds = [],
        #    SubnetIds = []
        #)

    ))

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

    # TODO: do we want to have fine-grained access control?
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