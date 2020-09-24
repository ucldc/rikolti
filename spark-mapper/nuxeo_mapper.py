import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 

def main(database, table):

    # Create a glue DynamicFrame
    original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
    
    # convert to apache spark dataframe
    original_DF = original_DyF.toDF() \
        .distinct()

    # register a user defined function for use by spark
    # FIXME write this in scala and call from python. starting a python process is expensive and it is moreover very costly to serialize the data to python
    spark.udf.register("map_rights_codes_py", map_rights_codes, StringType())

    # select columns that are a straight mapping
    transformed_DF = original_DF \
        .select(col('uid'), \
            col('properties.ucldc_schema:publisher'), \
            col('properties.ucldc_schema:alternativetitle'), \
            col('properties.ucldc_schema:extent'), \
            col('properties.ucldc_schema:publisher'), \
            col('properties.ucldc_schema:temporalcoverage'), \
            col('properties.dc:title'), \
            col('properties.ucldc_schema:type'), \
            col('properties.ucldc_schema:source'), \
            col('properties.ucldc_schema:provenance'), \
            col('properties.ucldc_schema:physlocation'), \
            col('properties.ucldc_schema:rightsstartdate'), \
            col('properties.ucldc_schema:transcription'), \
        )

    # process columns that need more complex unpacking/mapping
    date_df = map_date(original_DF)
    joinExpression = transformed_DF['uid'] == date_df['date_uid']
    transformed_DF = transformed_DF.join(date_df, joinExpression)

    rights_df = map_rights(original_DF)
    joinExpression = transformed_DF['uid'] == rights_df['rights_uid']
    transformed_DF = transformed_DF.join(rights_df, joinExpression)

    subject_df = map_subject(original_DF)
    joinExpression = transformed_DF['uid'] == subject_df['subject_uid']
    transformed_DF = transformed_DF.join(subject_df, joinExpression)
    
    # title needs to be repeatable
    # physdesc needs to be made into a struct
    # relatedresource

    # convert to glue dynamic frame
    transformed_DyF = DynamicFrame.fromDF(transformed_DF, glueContext, "transformed_DyF")

    # rename columns
    transformed_DyF = transformed_DyF.apply_mapping([
            ('uid', 'string', 'nuxeo_uid', 'string'),
            ('ucldc_schema:publisher', 'array', 'publisher', 'array'),
            ('ucldc_schema:alternativetitle', 'array', 'alternative_title', 'array'),
            ('ucldc_schema:extent', 'string', 'extent', 'string'),
            ('ucldc_schema:temporalcoverage', 'array', 'temporal', 'array'),
            ('dc:title', 'string', 'title', 'string'),
            ('ucldc_schema:type', 'string', 'type', 'string'),
            ('ucldc_schema:source', 'string', 'source', 'string'),
            ('ucldc_schema:provenance', 'array', 'provenance', 'array'),
            ('ucldc_schema:physlocation', 'string', 'location', 'string'),
            ('ucldc_schema:transcription', 'string', 'transcription', 'string'),
            ('date_mapped', 'array', 'date', 'array'),
            ('rights_mapped', 'array', 'rights', 'array'),
            ('subject_mapped', 'array', 'subject', 'array'),
        ])

    # write transformed data to target
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d_%H%M%S")
    path = "s3://ucldc-ingest/glue-test-data-target/{}/{}".format(table, dt_string)

    partition_keys = ["nuxeo_uid"] 
    glueContext.write_dynamic_frame.from_options(
       frame = transformed_DyF,
       connection_type = "s3",
       connection_options = {"path": path, "partitionKeys": partition_keys},
       format = "json")


def map_rights(dataframe):

    rights_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:rightsstatus'), col('properties.ucldc_schema:rightsstatement')) \
        .selectExpr('uid', 'map_rights_codes_py(`ucldc_schema:rightsstatus`)', '`ucldc_schema:rightsstatement`') \
        .select('uid', array('map_rights_codes_py(ucldc_schema:rightsstatus)', 'ucldc_schema:rightsstatement')) \
        .withColumnRenamed('uid', 'rights_uid') \
        .withColumnRenamed('array(map_rights_codes_py(ucldc_schema:rightsstatus), ucldc_schema:rightsstatement)', 'rights_mapped')
   

    return rights_df


def map_rights_codes(rights_str):
    '''Map the "coded" values of the rights status to a nice one for display
       This should really be a scala function which we call from python
    '''
    decoded = rights_str
    if rights_str == 'copyrighted':
        decoded = 'Copyrighted'
    elif rights_str == 'publicdomain':
        decoded = 'Public Domain'
    elif rights_str == 'unknown':
        decoded = 'Copyright Unknown'
    return decoded


def map_contributor(dataframe):

    pass


def map_creator(dataframe):

    pass


def map_date(dataframe):

    date_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:date')) \
        .withColumn('date_struct', explode(col('ucldc_schema:date'))) \
        .select('uid', 'date_struct.date') \
        .groupBy('uid') \
        .agg(collect_set('date')) \
        .withColumnRenamed('uid', 'date_uid') \
        .withColumnRenamed('collect_set(date)', 'date_mapped')

    return date_df


def map_subject(dataframe):

    subject_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:subjecttopic.heading'), col('properties.ucldc_schema:subjectname.name')) \
        .select('uid', array_union('heading', 'name')) \
        .withColumnRenamed('uid', 'subject_uid') \
        .withColumnRenamed('array_union(heading, name)', 'subject_mapped')

    return subject_df


if __name__ == "__main__":

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate()) 

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    sys.exit(main("test-data-fetched", "2020_03_19_0022"))   
