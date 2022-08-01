import sys
import json
from json.decoder import JSONDecodeError

from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import boto3

# from pyspark.sql import Row

calisphere_id = 'calisphere-id'
database = 'rikolti-test'

# ------------ GLUE JOB -------------- #
# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate()) 

spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

glue_client = boto3.client('glue', region_name='us-west-2')
rikolti_test_tables = glue_client.get_tables(DatabaseName=database)
rikolti_test_tables.keys()
# dict_keys(['TableList', 'NextToken', 'ResponseMetadata'])
rikolti_test_table_names = [(tb['Name'], tb['StorageDescriptor']['Columns']) for tb in rikolti_test_tables['TableList']]
rikolti_test_table_names = [n for n in rikolti_test_table_names if n[0][-5:] == 'datel']


# try to find those tables with "odd" publisher field type
# odd_publisher_tables = []
# while rikolti_test_tables.get('NextToken'):
#     rikolti_test_table_names = [(tb['Name'], tb['StorageDescriptor']['Columns']) for tb in rikolti_test_tables['TableList']]
#     rikolti_test_table_names = [n for n in rikolti_test_table_names if n[0][-5:] == 'datel']
#     print(len(rikolti_test_table_names))
#     publisher_tables = []
#     for tb in rikolti_test_table_names:
#         publisher_field = [field for field in tb[1] if field['Name'] == 'publisher']
#         if len(publisher_field) > 0:
#             publisher_tables.append((tb[0], publisher_field))
#     publisher_tables = [(tb[0], tb[1][0]['Type']) for tb in publisher_tables]
#     for tb in publisher_tables:
#         if tb[1] != 'array<struct<text():string>>':
#             odd_publisher_tables.append(tb)
#     rikolti_test_tables = glue_client.get_tables(DatabaseName=database, NextToken=rikolti_test_tables['NextToken'])



table = rikolti_test_table_names[0]

# Create a glue DynamicFrame
original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
# original_DyF = glueContext.create_dynamic_frame.from_catalog(database="pachamama-demo", table_name="4567_oac_datel")

# Resolve Choice fields before converting to data frame
# original_DyF = original_DyF.resolveChoice(specs = [("identifier[]", "make_struct")])
# original_DyF.toDF().select('identifier').withColumn('identifier_string', col('identifier.string')).show(10, truncate=False)

# convert to apache spark dataframe
oac_source = original_DyF.toDF().distinct()
# ------------ GLUE JOB -------------- #

### these fields directly map to a field of the same name
direct_fields = [
    'contributor',
    'creator', 
    'language',
    'publisher',
    # 'extent',     # don't seem to exist in source metadata
    # 'provenance'  # don't seem to exist in source metadata
]
# check if these field are represented in this source
direct_fields = [f for f in direct_fields if f in oac_source.columns]

if direct_fields:
    oac_mapped = oac_source.select(calisphere_id)
    for field in direct_fields:
        interstitial_df = (oac_source
            .select(calisphere_id, field)
            .withColumn(field, explode(field))
            .select(calisphere_id, col(f"{field}.text()").alias(field))
            .groupBy(calisphere_id)
            .agg(collect_list(field).alias(field))
        )
        oac_mapped = oac_mapped.join(interstitial_df, on=calisphere_id, how='left_outer')

# works for 10028_oac_datel which has a simple publisher
# but schema variance comparison shows that publisher can be complex
# find sample complex publisher
data = rikolti_test_tables['TableList']
data = [{'name': i['Name'], 'schema': i['StorageDescriptor']['Columns']} for i in data]
data = [d for d in data if d['name'][-9:] == 'oac_datel']
def abstract_group_by(items, group_by):
    unique = []
    while(len(items) > 0):
        remainder = []
        matched = []
        group = group_by(items[0])
        for item in items:
            if group_by(item) == group:
                matched.append(item)
            else:
                remainder.append(item)
        if group in unique:
            print('ERROR')
        unique.append({
            "group": group,
            "matched": matched
        })
        items = remainder
    return unique
publisher_schemas = abstract_group_by(data, lambda c: [f['Type'] for f in c['schema'] if f['Name'] == 'publisher'])
len(publisher_schemas)
# 1

# go get more table names



# THIS IS WHERE I STOPPED TRYING TO PUT TOGETHER A NEW OAC MAPPER 
# AND STARTED TO FOCUS ON HANDLING THE SCHEMA VARIANCE PRESENTED 
# BY THE PUBLISHER FIELD - SEE COMMENTED-OUT LINES AT 36-51














### these fields (1) are concatenated to the destination field (2)
combine_fields = [
    (['abstract', 'description', 'tableOfContents'], 'description'),
    (['identifier', 'bibliographicCitation'], 'identifier'),
    (['accessRights', 'rights'], 'rights')
]

def get_dtype(df, col):
    return [dtype for name,dtype in df.dtypes if name == col][0]


def get_source_field(df, src_field, exclusions=None, specifics=None):
    xml_df = df.select(calisphere_id, src_field)
    src_field_type = get_dtype(xml_df, src_field)

    if src_field_type.startswith('array<struct<'):
        xml_df = xml_df.withColumn(src_field, explode(src_field))
        xml_df = get_struct_field(xml_df, src_field, exclusions, specifics)
        xml_df = xml_df.groupBy(calisphere_id).agg(collect_list(src_field).alias(src_field))

    return xml_df

def get_struct_field(xml_df, src_field, exclusions=None, specifics=None):
    xml_df = (xml_df
        .select(
            calisphere_id, 
            col(f"{src_field}.text()").alias('text')
        )
    )

    xml_df = (xml_df
        .withColumn(
            src_field,
            when(col('text').isNotNull(), col('text')).otherwise(lit(None))
        )
    )
    xml_df = xml_df.drop('text')
    return xml_df


for source_fields, destination_field in combine_fields:
    # check if these fields are represented in this source
    source_fields = [f for f in source_fields if f in oac_source.columns]
    if source_fields:
        # coalesce allows us to concat even if one of source_fields is NULL
        # https://stackoverflow.com/questions/37284077/combine-pyspark-dataframe-arraytype-fields-into-single-arraytype-field
        
        source_df = oac_source.select(calisphere_id)
        for sf in source_fields:
            join_df = (get_source_field(oac_source, sf)
                .withColumn(sf, coalesce(col(sf), array()))
            )
            source_df = source_df.join(join_df, calisphere_id)

        combine_df = (source_df
            .select(
                calisphere_id, 
                concat(*source_fields).alias(destination_field)
            )
        )
        oac_mapped = oac_mapped.join(combine_df, calisphere_id)

    # these fields (0) exclude the specified subfield (1) 
    # when mapped to destination field (2)
    exclude_subfields = [
        ('date', 'dcterms:dateCopyrighted', 'date'),
        ('format', 'x', 'format'),
        ('title', 'alternative', 'title'),
        ('type', 'genreform', 'type'),
        ('subject', 'series', 'subject')
    ]

    for mapping in exclude_subfields:
        src = mapping[0]
        ex = mapping[1]
        dest = mapping[2]
        if src in oac_source.columns:
            join_df = get_source_field(oac_source, src, ex)
            if src != dest:
                join_df = (join_df
                    .withColumn(dest, col(src))
                    .drop(src)
                )
            join_df.show(10, truncate=False)
            oac_mapped = oac_mapped.join(join_df, calisphere_id)


    specific_subfield = [
        ('date', 'dcterms:dateCopyrighted', 'copyrightDate'),
        # ('coverage', '') coverage is complicated and not in this sample data - maps to spatial and temporal
        ('title', 'alternative', 'alternativeTitle'),
        ('type', 'genreform', 'genre')
    ]

    for mapping in specific_subfield:
        src = mapping[0]
        spec = mapping[1]
        dest = mapping[2]
        if src in oac_source.columns:
            join_df = get_source_field(oac_source, src, None, spec)
            if src != dest:
                join_df = (join_df
                    .withColumn(dest, col(src))
                    .drop(src)
                )
            # join_df.show(25, truncate=False)
            oac_mapped = oac_mapped.join(join_df, calisphere_id)

    add_fields = ('{"name": "California"}', 'stateLocatedIn')
    oac_mapped = oac_mapped.withColumn(add_fields[1], lit(add_fields[0]))

# ------------ LOCAL DEV -------------- #
    # oac_mapped.show(25)
    # oac_mapped.printSchema()
# ------------ LOCAL DEV -------------- #

# ------------ GLUE JOB -------------- #
    # convert to glue dynamic frame
    transformed_DyF = DynamicFrame.fromDF(oac_mapped, glueContext, "transformed_DyF")

    # write transformed data to target
    now = datetime.now()
    collection_id = '509'
    dt_string = now.strftime("%Y-%m-%d")
    path = "s3://ucldc-ingest/glue-test-data-target/mapped/{}".format(dt_string)

    partition_keys = [calisphere_id] 
    glueContext.write_dynamic_frame.from_options(
       frame = transformed_DyF,
       connection_type = "s3",
       connection_options = {"path": path, "partitionKeys": partition_keys},
       format = "json")
# ------------ GLUE JOB -------------- #






# ------------ GLUE JOB -------------- #
if __name__ == "__main__":

    # Create a Glue context
    glueContext = GlueContext(SparkContext.getOrCreate()) 

    spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

    sys.exit(main("pachamama-demo", "oac509"))
# ------------ GLUE JOB -------------- #

# ------------ LOCAL DEV -------------- #
# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: pyspark-oac-mapper.py <file>", file=sys.stderr)
#         sys.exit(-1)

#     sys.exit(main(sys.argv[1]))
# ------------ LOCAL DEV -------------- #





