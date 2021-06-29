import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *

'''

   Join output of mapper and textract jobs

   See "Code Example: Joining and Relationalizing Data": https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-samples-legislators.html
   See also "How can I run an AWS Glue Job on a specific partition in Amazon S3": https://aws.amazon.com/premiumsupport/knowledge-center/glue-job-specific-s3-partition/

'''
def main(database, collection_id):

    # create glue DynamicFrames
    mapped_metadata_DyF = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name='mapped_metadata',
        push_down_predicate=f"(collection_id == '{collection_id}')")

    textract_output_DyF = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name='textract',
        push_down_predicate=f"(collection_id == '{collection_id}')")


    mapped_metadata_df = mapped_metadata_DyF.toDF().distinct()
    textract_df = textract_output_DyF.toDF().distinct()

    if textract_df.head():
        textract_df = textract_df.drop('textract_job', 'collection_id')
        joined_df = mapped_metadata_df.join(
            textract_df, "calisphere-id", "left_outer")
    else:
        joined_df = mapped_metadata_df

    joined_dyf = DynamicFrame.fromDF(
        joined_df, glueContext, "joined_dyf")

    # write data to target
    path = "s3://rikolti/joined/"

    partition_keys = ["collection_id"]
    glueContext.write_dynamic_frame.from_options(
       frame = joined_dyf,
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

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'collection_id'])

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate())

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    print(f"Joining {args['collection_id']}")
    main("rikolti", args['collection_id'])

