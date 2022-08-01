# TO RUN:
# (pyspark) (oac-fetcher) Amys-MBP:spark-mapper amywieliczka$ pwd
# /Users/amywieliczka/Projects/work/harvesting/rikolti/spark-mapper
# (pyspark) (oac-mapper) Amys-MBP:spark-mapper amywieliczka$ spark-submit pyspark-oac-mapper.py 509/2021-03-22/0.jsonl 

import sys
import json
from json.decoder import JSONDecodeError

from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.sql import SparkSession
from pyspark.sql.functions import (when, coalesce, col, array, concat, udf,
                                   lit, explode, collect_list)
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.utils import AnalysisException


# from pyspark.sql import Row

calisphere_id = 'calisphere-id'


def main(database, table):
    # Create a glue DynamicFrame
    original_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=database, table_name=table)

    # Resolve Choice fields before converting to data frame
    original_dyf = original_dyf.resolveChoice(choice="make_struct")
    # original_dyf = original_dyf.resolveChoice(
    #     specs=[("identifier[]", "make_struct"), ("subject[]", "make_struct")])

    # convert to apache spark dataframe
    oac_source = original_dyf.toDF().distinct()

    # these fields directly map to a field of the same name
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

    # these fields (1) are concatenated to the destination field (2)
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
            combine_df.show(10, False)
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

    # oac_mapped.show(25)
    # oac_mapped.printSchema()

    oac_mapped = oac_mapped.withColumn('collection_id', lit(table))
    return oac_mapped


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False


def get_dtype(df, col):
    return [dtype for name, dtype in df.dtypes if name == col][0]


def get_dtype_class(df, col):
    return [t.dataType for t in df.schema if t.name == col][0]


def get_text_from_string(src, exclusion=None, specific=None):
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
    if not src_arr:
        return src_arr
    
    field = []
    for src in src_arr:
        if not src:
            continue

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
    get_text_from_string_udf = udf(
        get_text_from_string, ArrayType(StringType()))
    # spark.udf.register('get_text_from_string_udf', get_text_from_string, ArrayType(StringType()))

    xml_df = (xml_df
        .withColumn(
            src_field,
            get_text_from_string_udf(src_field, lit(exclusions), lit(specifics))
        )
    )
    return xml_df


def get_struct_field(xml_df, src_field, exclusions=None, specifics=None):
    sub_field_names = [f.name for f in xml_df.select(f"{src_field}.*").schema]

    if "@q" in sub_field_names:
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
                    when(col('attrib') != lit(exclusions), col('text')).otherwise(
                        lit(None))
                )
            )
        elif specifics:
            xml_df = (xml_df
                .withColumn(
                    src_field,
                    when(col('attrib') == lit(specifics), col('text')).otherwise(
                        lit(None))
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

    else:
        xml_df = (xml_df
            .select(
                calisphere_id,
                col(f"{src_field}.#text").alias('text')
            )
            .withColumn(
                src_field,
                when(col('text').isNotNull(), col('text')).otherwise(lit(None))
            )
        )

    return xml_df


def get_array_field(xml_df, src_field, exclusions=None, specifics=None):
    get_text_from_array_udf = udf(get_text_from_array, ArrayType(StringType()))

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
    src_field_class = get_dtype_class(xml_df, src_field)

    if src_field_type == 'string':
        xml_df = get_string_field(xml_df, src_field, exclusions, specifics)
    elif isinstance(src_field_class, StructType):
        # check for presence of any of these: array, struct, string
        # this is a resolve choice field from Glue ChoiceType
        resolve_choice_fields = list(
            set(["array", "struct", "string"]) &
            set([f.name for f in xml_df.select(f"{src_field}.*").schema])
        )
    elif src_field_type == "array<struct<#text:string,@q:string>>" or src_field_type == "array<struct<@q:string,#text:string>>":
        xml_df = xml_df.withColumn(src_field, explode(src_field))
        xml_df = get_struct_field(xml_df, src_field, exclusions, specifics)
        # xml_df.show(10, truncate=False)
        xml_df = xml_df.groupBy(calisphere_id).agg(collect_list(src_field).alias(src_field))
    elif src_field_type == "array<string>":
        xml_df = get_array_field(xml_df, src_field, exclusions, specifics)
    elif src_field_type == "array<struct<string:string,struct:struct<@q:string,#text:string>>>":
        xml_df = (xml_df
            .withColumn(f"{src_field}_string", col(f"{src_field}.string"))
            .withColumn(f"{src_field}_struct", col(f"{src_field}.struct"))
        )
        str_field = get_source_field(xml_df, f"{src_field}_string", exclusions, specifics)
        struct_field = get_source_field(xml_df, f"{src_field}_struct", exclusions, specifics)
        xml_df = struct_field.join(str_field, calisphere_id)
        xml_df = (xml_df
            .select(
                calisphere_id,
                concat(f"{src_field}_string",f"{src_field}_struct").alias(src_field)
            )
        )

    return xml_df

# ------------ LOCAL DEV -------------- #
# def main(oac_file):
#     spark = (SparkSession
#         .builder
#         .appName("OAC Mapper")
#         .getOrCreate())
#     oac_source = (spark.read.format("json")
#         .option("header", True)
#         .option("inferSchema", True)
#         .load(oac_file))
#     oac_source.printSchema()
# ------------ LOCAL DEV -------------- #

# ------------ GLUE JOB -------------- #
def main(database, table):
    # Create a glue DynamicFrame
    original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
    # Resolve Choice fields before converting to data frame
    original_DyF = original_DyF.resolveChoice(specs = [("identifier[]", "make_struct")])
    # original_DyF.toDF().select('identifier').withColumn('identifier_string', col('identifier.string')).show(10, truncate=False)

    # convert to apache spark dataframe
    oac_source = original_DyF.toDF() \
        .distinct()
# ------------ GLUE JOB -------------- #

    ### these fields directly map to a field of the same name
    direct_fields = [
        'contributor',
        'creator', 
        # 'extent',
        'language',
        'publisher',
        # 'provenance'
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
                choice_df.show(10, truncate=True)
                result_field = get_source_field(choice_df, f"{src_field}_{field}", exclusions, specifics)
                result_field.show(10, truncate=True)
                xml_df = result_field.join(xml_df, calisphere_id)

            xml_df.show(10, truncate=True)
            concat_fields = [f"{src_field}_{rcf}" for rcf in resolve_choice_fields]
            xml_df = (xml_df
                .select(
                    calisphere_id,
                    concat(*concat_fields).alias(src_field)
                )
            )
            xml_df.show(10, truncate=True)
        else:
            xml_df = (get_struct_field(xml_df, src_field, exclusions, specifics)
                .withColumn(src_field, array(col(src_field)))
            )
            xml_df.show(10, truncate=True)
    elif isinstance(src_field_class, ArrayType):

        if isinstance(src_field_class.elementType, StringType):
            xml_df = get_array_field(xml_df, src_field, exclusions, specifics)
        if isinstance(src_field_class.elementType, StructType):
            xml_df = xml_df.withColumn(src_field, explode(src_field))
            xml_df = get_struct_field(xml_df, src_field, exclusions, specifics)
            # xml_df.show(10, truncate=False)
            xml_df = (xml_df
                .groupBy(calisphere_id)
                .agg(collect_list(src_field).alias(src_field))
            )

    return xml_df


if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'collection_id'])

    # Create a Glue context
    glue_context = GlueContext(SparkContext.getOrCreate())

    # SparkSession provided with GlueContext. Pass this around
    # at runtime rather than instantiating within every python class
    spark = glue_context.spark_session

    print(args['collection_id'])
    oac_mapped = main("rikolti", args['collection_id'])

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

