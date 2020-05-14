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
    original_DF = original_DyF.toDF()

    # transform 
    transformed_DF = ucldc_transform(original_DF)
    #transformed_DF.show()

def ucldc_transform(dataframe):

    new_df = dataframe \
        .withColumn('publisher_mapped', map_publisher(dataframe)) \
        .withColumn('title_mapped', map_title(dataframe)) \
        .withColumn('date_mapped', map_date(dataframe)) \


    #    .withColumn('subject_mapped', map_subject(dataframe))


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

    #properties_df = dataframe.select('properties') # spark type is struct
    #properties_df.createOrReplaceTempView('properties_df')
    #date_df = properties_df.select('properties.ucldc_schema:date') # can only use dot notation on struct type 
    #date_df = dataframe.select(col('uid'), col('properties.ucldc_schema:date'))
    #date_df.show()

    #date_df_exploded = date_df.withColumn('date_struct', explode(col('ucldc_schema:date')))

    date_df_exploded = dataframe.select(col('uid'), col('properties.ucldc_schema:date')) \
        .withColumn('date_struct', explode(col('ucldc_schema:date')))

    date_df_exploded.show()
    date_df_date_extracted = date_df_exploded.select('uid', 'date_struct.date')
    date_df_date_extracted.show() # a dataframe containing 'uid' and 'date'; can be multiple rows per uid

    # now assemble an array of dates for each uid and make it one row again
 
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
   
    date_col = dataframe['properties.ucldc_schema:date'] #FIXME
    
    '''
     |    |-- ucldc_schema:date: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- date: string (nullable = true)
     |    |    |    |-- inclusivestart: string (nullable = true)
     |    |    |    |-- single: string (nullable = true)
     |    |    |    |-- datetype: string (nullable = true)
     |    |    |    |-- inclusiveend: string (nullable = true)
    '''
  
    # dates = [date['date'] for date in date_col] 
    return date_col 

def map_subject(dataframe):

    subject_df = dataframe.select(col('uid'), col('properties.ucldc_schema:subjecttopic'))
    subject_df_exploded = subject_df.withColumn('subject_exploded', explode(col('ucldc_schema:subjecttopic')))
    
    #subject_df_exploded.show()


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

    sys.exit(main("test-data-fetched", "466"))   
