import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import pprint
pp = pprint.PrettyPrinter(indent=4)

def main(database, table):

    # Create a glue DynamicFrame
    original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
    
    # convert to apache spark dataframe
    original_DF = original_DyF.toDF() \
        .distinct()

    new_df = original_DF \
        .select(col('uid'))

    date_df = original_DF \
        .select(col('uid'), col('properties.ucldc_schema:date')) \
        .withColumn('date_struct', explode(col('ucldc_schema:date'))) \
        .select('uid', 'date_struct.date') \
        .groupBy('uid') \
        .agg(collect_set('date')) \
    
    #uid_df = original_DF.select(col('uid'))
    #uid_df.show(n=300, truncate=False)

    joinExpression = new_df['uid'] == date_df['uid']
    new_df = new_df.join(date_df, joinExpression)
    new_df.show(n=300, truncate=False)

def ucldc_transform(dataframe):

    new_df = dataframe \
        .withColumn('publisher_mapped', map_publisher(dataframe)) \
        .withColumn('title_mapped', map_title(dataframe)) \
        .withColumn('date_mapped', map_date(dataframe))

    return new_df

def map_publisher(dataframe):

    publisher_col = dataframe['properties.ucldc_schema:publisher']
    return publisher_col

def map_rights(dataframe):
    # need to merge the contents of 2 columns
    #rights_col = dataframe['properties.ucldc_schema:rightsstatus']
    return
    
def map_title(dataframe):
    title_col = dataframe['properties.dc:title']
    return title_col    

def map_date(dataframe):

    '''
    root
     |-- ucldc_schema:date: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- date: string (nullable = true)
     |    |    |-- inclusivestart: string (nullable = true)
     |    |    |-- single: string (nullable = true)
     |    |    |-- datetype: string (nullable = true)
     |    |    |-- inclusiveend: string (nullable = true)
    '''
  
    date_extracted_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:date')) \
        .withColumn('date_struct', explode(col('ucldc_schema:date'))) \
        .select('uid', 'date_struct.date') \
        .groupBy('uid', 'date') \
        .agg(collect_set('date'))
    date_extracted_df.show(n=50, truncate=False)

    date_col = date_extracted_df['collect_set(date)']
 
    return date_col 

def map_subject(dataframe):

    '''
 |    |-- ucldc_schema:subjecttopic: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- headingtype: string (nullable = true)
 |    |    |    |-- authorityid: string (nullable = true)
 |    |    |    |-- heading: string (nullable = true)
 |    |    |    |-- source: string (nullable = true)
    '''

    properties_df = dataframe \
        .select(col('uid'), col('properties.ucldc_schema:subjecttopic')) \
    #    .withColumnRenamed('ucldc_schema:subjecttopic', 'subjecttopic2')
    
    subject_col = properties_df['.ucldc_schema:subjecttopic']
    #subject_df = dataframe_tempview \
    #    .select(col('uid'), col('properties.ucldc_schema:subjecttopic'))

    #print("type(subject_df): {}".format(type(subject_df)))
    #subject_df = subject_df.createOrReplaceTempView("subject_df")
    
    #spark.catalog.dropTempView("dataframe") 
    #spark.catalog.dropTempView("subject_df")

    '''
    subject_df \
        .show(truncate=False)
    '''

    
    #subject_col = subject_df['ucldc_schema:subjecttopic']

    '''
    subject_df_exploded = subject_df \
        .withColumn('subject_exploded', explode(col('ucldc_schema:subjecttopic'))) \
        .show(truncate=False)
    '''

    #subject_df_exploded.show()

    return subject_col


def forlater():
    #print("Count:  ", testdata_DyF.count()) 
    #testdata_DyF.printSchema()
    #testdata_DyF.show() # glue dynamic frame. dict like output.

    #properties_DyF = testdata_DyF.select_fields(['properties'])
    #properties_DyF.show()
    #properties_DyF.printSchema()

    # create a temp table to do sql queries on the data
    # problem is we can't access the nested data very easily this way
    '''
    testdata_DF.createOrReplaceTempView("data")
    sql_DF = glueContext.spark_session.sql("SELECT `properties` FROM data") # this doesn't actually execute anything? It's just a plan.
    sql_DF.show() # this is an action?
    sql_DF.explain()  
    '''

    # get a nested field as a column from the dataframe
    '''
    chop_f = udf(lambda x: x[1:], StringType())
    titleCol = testdata_DF['properties.dc:title'] # this is a Column object
    testdata_DF = testdata_DF.withColumn('title_mapped', chop_f(titleCol)).show()
    '''
    #new_DF = testdata_DF.select(testdata_DF['uid']).withColumnRenamed('uid', 'harvest_id').withColumn("test", testdata_DF['properties.dc:title']).show()
    #new_DF.show()

    #newer_DF = new_DF.withColumnRenamed('uid', 'harvest_id')
#newer_DF.show()

if __name__ == "__main__":

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate()) 

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    sys.exit(main("test-data-fetched", "2020_03_19_0022"))   
