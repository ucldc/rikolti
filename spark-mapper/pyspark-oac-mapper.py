# TO RUN:
# (pyspark) (oac-fetcher) Amys-MBP:spark-mapper amywieliczka$ pwd
# /Users/amywieliczka/Projects/work/harvesting/rikolti/spark-mapper
# (pyspark) (oac-mapper) Amys-MBP:spark-mapper amywieliczka$ spark-submit pyspark-oac-mapper.py 509/2021-03-09/0.jsonl 

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

# from pyspark.sql import Row

calisphere_id = 'calisphere-id'

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def get_dtype(df, col):
    return [dtype for name,dtype in df.dtypes if name == col][0]

def get_text_from_string(src, exclusion=None, specific=None):
    # print(f"source: {src}")
    # print(f"exclude: {exclusion}")
    # print(f"specify: {specific}")
    if not src:
        return src
    
    try:
        j = json.loads(src)
    # it really is a string
    except JSONDecodeError:
        if not specific:
            return [src]
        else:
            return None

    field = []
    if isinstance(j, dict):
        if exclusion:
            if j.get('@q') != exclusion:
                field.append(j.get('#text'))
        elif specific:
            if j.get('@q') == specific:
                field.append(j.get('#text'))
        else:
            field.append(j.get('#text'))
    elif isinstance(j, list):
        for elem in j:
            if isinstance(elem, str):
                if not specific:
                    field.append(elem)
            elif isinstance(elem, dict):
                if exclusion:
                    if elem.get('@q') != exclusion:
                        field.append(elem.get('#text'))
                elif specific:
                    if elem.get('@q') == specific:
                        field.append(elem.get('#text'))
                else:
                    field.append(elem.get('#text'))
            elif isinstance(elem, list):
                print('whoa nested list')
    else:
        print('unknown')


    if len(field) == 0:
        field = None

    return field

def get_text_from_array(src_arr, exclusion=None, spec=None):
    # print(f"source: {src_arr}")
    # print(f"exclude: {exclusion}")
    # print(f"specify: {spec}")
    if not src_arr:
        return src_arr
    
    field = []
    for src in src_arr:
        try:
            j = json.loads(src)
        except JSONDecodeError:
            # it's a string
            if not spec:
                field.append(src)
            continue

        if isinstance(j, dict):
            if exclusion:
                if j.get('@q') != exclusion:
                    field.append(j.get('#text'))
            elif spec:
                if j.get('@q') == spec:
                    field.append(j.get('#text'))
            else:
                field.append(j.get('#text'))
        elif isinstance(j, list):
            print('whoa nested list')
        else:
            print('unknown')


    if len(field) == 0:
        field = None

    return field


def get_string_field(xml_df, src_field, exclusions=None, specifics=None):
    get_text_from_string_udf = udf(get_text_from_string, ArrayType(StringType()))
    # spark.udf.register('get_text_from_string_udf', get_text_from_string, ArrayType(StringType()))
    xml_df = (xml_df
        .withColumn(
            src_field,
            get_text_from_string_udf(src_field, lit(exclusions), lit(specifics))
        )
    )
    return xml_df

def get_struct_field(xml_df, src_field, exclusions=None, specifics=None):
    xml_df = (xml_df
        .select(
            calisphere_id, 
            col(f"{src_field}.@q").alias('attrib'), 
            col(f"{src_field}.#text").alias('text')
        )
    )
    if exclusions:
        xml_df = (xml_df
            .withColumn(
                src_field,
                when(col('attrib') != lit(exclusions), col('text')).otherwise(lit(None))
            )
        )
    elif specifics:
        xml_df = (xml_df
            .withColumn(
                src_field,
                when(col('attrib') == lit(specifics), col('text')).otherwise(lit(None))
            )
        )
    else:
        xml_df = (xml_df
            .withColumn(
                src_field,
                when(col('text').isNotNull(), col('text')).otherwise(lit(None))
            )
        )
    xml_df = xml_df.drop('attrib', 'text')
    return xml_df

def get_array_field(xml_df, src_field, exclusions=None, specifics=None):
    get_text_from_array_udf = udf(get_text_from_array, ArrayType(StringType()))
    # spark.udf.register('get_text_from_array_udf', get_text_from_array, ArrayType(StringType()))

    xml_df = (xml_df
        .withColumn(
            src_field,
            get_text_from_array_udf(src_field, lit(exclusions), lit(specifics))
        )
    )
    return xml_df

def get_source_field(df, src_field, exclusions=None, specifics=None):
    xml_df = df.select(calisphere_id, src_field)
    src_field_type = get_dtype(xml_df, src_field)
    if src_field_type == 'string':
        xml_df = get_string_field(xml_df, src_field, exclusions, specifics)
    elif src_field_type.startswith('struct'):
        xml_df = (get_struct_field(xml_df, src_field, exclusions, specifics)
            .withColumn(src_field, array(col(src_field)))
        )
    elif src_field_type == "array<struct<#text:string,@q:string>>":
        xml_df = xml_df.withColumn(src_field, explode(src_field))
        xml_df = get_struct_field(xml_df, src_field, exclusions, specifics)
        xml_df = xml_df.groupBy(calisphere_id).agg(collect_list(dest).alias(dest))
    elif src_field_type == "array<string>":
        xml_df = get_array_field(xml_df, src_field, exclusions, specifics)

    return xml_df

# ------------ LOCAL DEV -------------- #
# def main(oac_file):
    # spark = (SparkSession
    #     .builder
    #     .appName("OAC Mapper")
    #     .getOrCreate())
    # oac_source = (spark.read.format("json")
    #     .option("header", True)
    #     .option("inferSchema", True)
    #     .load(oac_file))
    # oac_source.printSchema()
# ------------ LOCAL DEV -------------- #

# ------------ GLUE JOB -------------- #
def main(database, table):
    # Create a glue DynamicFrame
    original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)

    # convert to apache spark dataframe
    oac_source = original_DyF.toDF() \
        .distinct()
# ------------ GLUE JOB -------------- #

    ### these fields directly map to a field of the same name
    direct_fields = [
        'contributor',
        'creator', 
        'extent',
        'language',
        'publisher',
        'provenance'
    ]

    # check if these field are represented in this source
    direct_fields = [f for f in direct_fields if f in oac_source.columns]
    if direct_fields:
        direct_fields.append(calisphere_id)
        oac_mapped = oac_source.select(direct_fields)
    

    ### these fields (1) are concatenated to the destination field (2)
    combine_fields = [
        (['abstract', 'description', 'tableOfContents'], 'description'),
        (['identifier', 'bibliographicCitation'], 'identifier'),
        (['accessRights', 'rights'], 'rights')
    ]

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
            join_df = (get_source_field(oac_source, src, ex)
                .withColumn(dest, col(src))
                .drop(src)
            )
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
            join_df = (get_source_field(oac_source, src, None, spec)
                .withColumn(dest, col(src))
                .drop(src)
            )
            # join_df.show(25, truncate=False)
            oac_mapped = oac_mapped.join(join_df, calisphere_id)

    add_fields = ('{"name": "California"}', 'stateLocatedIn')
    oac_mapped = oac_mapped.withColumn(add_fields[1], lit(add_fields[0]))

    # oac_mapped.show(25)
    # oac_mapped.printSchema()

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





