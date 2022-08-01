import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

import oac_mapper.main as oac_main

if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'collection_id'])

    # Create a Glue context
    glue_context = GlueContext(SparkContext.getOrCreate())

    # SparkSession provided with GlueContext. Pass this around
    # at runtime rather than instantiating within every python class
    spark = glue_context.spark_session

    print(args['collection_id'])
    oac_mapped = oac_main("rikolti", args['collection_id'])

    oac_mapped.drop('publisher')

    # convert to glue dynamic frame
    transformed_dyf = DynamicFrame.fromDF(
        oac_mapped, glue_context, "transformed_dyf")

    # write transformed data to target
    path = "s3://rikolti/mapped_metadata/"

    partition_keys = ["collection_id"]
    glue_context.write_dynamic_frame.from_options(
       frame=transformed_dyf,
       connection_type="s3",
       connection_options={"path": path, "partitionKeys": partition_keys},
       format="json")
