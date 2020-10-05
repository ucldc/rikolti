import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from datetime import datetime

'''

   Join output of mapper and textract jobs

   See "Code Example: Joining and Relationalizing Data": https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-samples-legislators.html

'''
def main(database, collection_id):

    # create glue DynamicFrames
    metadata_table = 'mapped'
    mapped_metadata_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=metadata_table)

    textract_table = 'textract'
    textract_output_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=textract_table)


    # join tables together on calisphere-id
    joined_DyF = Join.apply(mapped_metadata_DyF, textract_output_DyF, 'calisphere-id', 'calisphere-id')

    # remove duplicate `.calisphere-id` field
    joined_DyF = joined_DyF.drop_fields(['`.calisphere-id`'])

    # write data to target
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d")
    path = "s3://ucldc-ingest/glue-test-data-target/joined/{}".format(collection_id, dt_string)

    partition_keys = ["nuxeo_uid"]
    glueContext.write_dynamic_frame.from_options(
       frame = joined_DyF,
       connection_type = "s3",
       connection_options = {"path": path, "partitionKeys": partition_keys},
       format = "json")

if __name__ == "__main__":

    '''
    parser = argparse.ArgumentParser(
        description='Run glue spark joiner')
    parser.add_argument('buckets', help='list of S3 buckets')
    parser.add_argument('prefix', help='S3 prefix')
    parser.add_argument('index', help='ES index name')

    argv = parser.parse_args()

    bucket = argv.bucket
    prefix = argv.prefix
    index = argv.index
    '''

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate())

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    sys.exit(main("pachamama-test", "466"))
