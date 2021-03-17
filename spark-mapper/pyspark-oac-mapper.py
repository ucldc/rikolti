# TO RUN:
# (pyspark) (oac-fetcher) Amys-MBP:spark-mapper amywieliczka$ pwd
# /Users/amywieliczka/Projects/work/harvesting/rikolti/spark-mapper
# (pyspark) (oac-mapper) Amys-MBP:spark-mapper amywieliczka$ spark-submit pyspark-oac-mapper.py 509/2021-03-09/0.jsonl 

import sys
import json
from json.decoder import JSONDecodeError

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

# from pyspark.sql import Row

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def get_dtype(df, col):
    return [dtype for name,dtype in df.dtypes if name == col][0]

def get_text(source_arr, exclusions=None):
    new_array = None
    attribute = None
    for val in source_arr:
        if val:
            try:
                j = json.loads(val)
                new_val = j['#text']
                attribute = j['@q']
            except JSONDecodeError:
                new_val = val

            if exclusions and attribute and attribute == exclusions:
                continue

            if not new_array:
                new_array = []
            new_array.append(new_val)

    return new_array


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pyspark-oac-mapper.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("OAC Mapper")
        .getOrCreate())
    oac_file=sys.argv[1]
    oac_source = (spark.read.format("json")
        .option("header", True)
        .option("inferSchema", True)
        .load(oac_file))
    # oac_source.printSchema()


    # SECOND IDEA, WORKING, EXPLODE ARRAY, GET JSON OBJECT BY NAME (EXPLICIT SCHEMA)
    # oac_ids = (oac_source
    #     .select('calisphere-id',explode('identifier').alias('identifier'))
    #     .select(
    #         'calisphere-id',
    #         get_json_object('identifier', '$.#text').alias('id_text'),
    #         get_json_object('identifier', '$.@q').alias('id_q'),
    #         'identifier'
    #     )
    # )

    # oac_ids.show(10, truncate=False)
    # oac_ids.printSchema()

    
    # ONE IDEA, CONVERT ARRAY TO STRING, READ AS JSON TO GET SCHEMA, LOAD AS JSON
    # text_fields = spark.read.json().schema
    # oac_ids = (oac_source
    #     .select('identifier')
    #     .withColumn('id_json', from_json('identifier', text_fields))
    #     .show(10, truncate=False)
    # )


    if has_column(oac_source, 'identifier'):
        id_dtype = get_dtype(oac_source, 'identifier')
        if id_dtype == 'string':
            oac_source = oac_source.withColumn('calisphere-id', col('identifier'))
        elif id_dtype.startswith('array'):
            id_array_type = id_dtype[6:-1]
            if id_array_type == 'string':
                oac_source = (oac_source.withColumn(
                    'calisphere-id', col('identifier').getItem(0)))

    calisphere_id = 'calisphere-id'

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

    get_text_udf = udf(get_text, ArrayType(StringType()))

    for source_fields, destination_field in combine_fields:
        # check if these field are represented in this source
        source_fields = [f for f in source_fields if f in oac_source.columns]
        if source_fields:
            # coalesce allows us to concat even if one of source_fields is NULL
            # https://stackoverflow.com/questions/37284077/combine-pyspark-dataframe-arraytype-fields-into-single-arraytype-field
            coalesce_f = []
            for sf in source_fields:
                if get_dtype(oac_source, sf).startswith('array'):
                    coalesce_f.append(coalesce(col(sf), array()))
                else:
                    coalesce_f.append(coalesce(array(col(sf)), array()))

            combine_df = (oac_source
                .select(calisphere_id, concat(*coalesce_f).alias('tmp'))
                .withColumn(destination_field, get_text_udf('tmp'))
                .drop('tmp'))
            # combine_df.show(25, truncate=False)
            oac_mapped = oac_mapped.join(combine_df, calisphere_id)

    """ these fields (0) exclude the specified subfield (1) 
    when mapped to destination field (2) """
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
            if get_dtype(oac_source, src) == 'struct<#text:string,@q:string>':
                # https://sparkbyexamples.com/pyspark/pyspark-when-otherwise/
                exclusion = (oac_source
                    .select(calisphere_id, 
                        col(f"{src}.#text").alias('text'), 
                        col(f"{src}.@q").alias('attrib'))
                    .withColumn(dest, 
                        when(col('attrib') != ex, array(col('text')))
                        .otherwise(array()))
                    )
                exclusion = exclusion.select(calisphere_id, dest)
                oac_mapped = oac_mapped.join(exclusion, calisphere_id)
            if get_dtype(oac_source, src) == 'array<struct<#text:string,@q:string>>':
                exclusion = (oac_source
                    .select(calisphere_id,
                        explode(src).alias(f"{src}-exploded"))
                    .select(calisphere_id,
                        col(f"{src}-exploded.#text").alias('text'),
                        col(f"{src}-exploded.@q").alias('attrib'))
                    .withColumn(dest, 
                        when(col('attrib') != ex, 
                            array(col('text'))).otherwise(array()))
                    .groupBy(calisphere_id)
                    .agg(collect_list(dest).alias(dest))
                )
                oac_mapped = oac_mapped.join(exclusion, calisphere_id)
            if get_dtype(oac_source, src) == 'string':
                direct = oac_source.select(calisphere_id, src)
                oac_mapped = oac_mapped.join(direct, calisphere_id)

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
            if get_dtype(oac_source, src) == 'string':
                # this attribute is only present if the string contains json
                continue;
            if get_dtype(oac_source, src) == 'struct<#text:string,@q:string>':
                specific = (oac_source
                    .select(calisphere_id,
                        col(f"{src}.#text").alias('text'),
                        col(f"{src}.@q").alias('attrib'))
                    .withColumn(dest, 
                        when(col('attrib') == spec, 
                            array(col('text'))).otherwise(array()))
                    .select(calisphere_id, dest)
                )
                oac_mapped = oac_mapped.join(specific, calisphere_id)

    add_fields = ('{"name": "California"}', 'stateLocatedIn')
    oac_mapped = oac_mapped.withColumn(add_fields[1], lit(add_fields[0]))

    oac_mapped.show(25)
    oac_mapped.printSchema()


