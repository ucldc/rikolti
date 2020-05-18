import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 

def main(database, table):

    # Create a glue DynamicFrame
    original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
    
    # convert to apache spark dataframe
    original_DF = original_DyF.toDF() \
        .distinct()

    spark.udf.register("map_rights_codes_py", map_rights_codes, StringType())

    transformed_DF = original_DF \
        .select(col('uid'), \
            col('properties.ucldc_schema:publisher'), \
            col('properties.ucldc_schema:creator'), \
            col('properties.ucldc_schema:alternativetitle'), \
            col('properties.ucldc_schema:extent'), \
            col('properties.ucldc_schema:physdesc'), \
            col('properties.ucldc_schema:publisher'), \
            col('properties.ucldc_schema:relatedresource'), \
            col('properties.ucldc_schema:temporalcoverage'), \
            col('properties.dc:title'), \
            col('properties.ucldc_schema:type'), \
            col('properties.ucldc_schema:source'), \
            col('properties.ucldc_schema:provenance'), \
            col('properties.ucldc_schema:physlocation'), \
            col('properties.ucldc_schema:rightsstartdate'), \
            col('properties.ucldc_schema:transcription'), \
        )

    date_df = map_date(original_DF)
    joinExpression = transformed_DF['uid'] == date_df['date_uid']
    transformed_DF = transformed_DF.join(date_df, joinExpression)

    rights_df = map_rights(original_DF)
    joinExpression = transformed_DF['uid'] == rights_df['rights_uid']
    transformed_DF = transformed_DF.join(rights_df, joinExpression)

    subject_df = map_subject(original_DF)
    joinExpression = transformed_DF['uid'] == subject_df['subject_uid']
    transformed_DF = transformed_DF.join(subject_df, joinExpression)

    transformed_DF.show(n=300, truncate=False)
    #transformed_DF.printSchema()

def map_rights(dataframe):

    rights_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:rightsstatus'), col('properties.ucldc_schema:rightsstatement')) \
        .selectExpr('uid', 'map_rights_codes_py(`ucldc_schema:rightsstatus`)', '`ucldc_schema:rightsstatement`') \
        .select('uid', array('map_rights_codes_py(ucldc_schema:rightsstatus)', 'ucldc_schema:rightsstatement')) \
        .withColumnRenamed('uid', 'rights_uid')

    return rights_df

def map_rights_codes(rights_str):
    '''Map the "coded" values of the rights status to a nice one for display
       This should really be a scala function which we call from python
    '''
    print(type(rights_str))
    print('rights_str: {}'.format(rights_str))
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

def map_date(dataframe):

    date_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:date')) \
        .withColumn('date_struct', explode(col('ucldc_schema:date'))) \
        .select('uid', 'date_struct.date') \
        .groupBy('uid') \
        .agg(collect_set('date')) \
        .withColumnRenamed('uid', 'date_uid')

    return date_df


def map_subject(dataframe):

    subject_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:subjecttopic.heading'), col('properties.ucldc_schema:subjectname.name')) \
        .select('uid', array_union('heading', 'name')) \
        .withColumnRenamed('uid', 'subject_uid')

    return subject_df


if __name__ == "__main__":

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate()) 

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    sys.exit(main("test-data-fetched", "2020_03_19_0022"))   
