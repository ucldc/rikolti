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

def get_values_from_text_attrib(source_data, suppress_attribs={}):
    values = []
    if not source_data:
        return source_data

    for x in source_data:
        try:
            x = json.loads(x)
        except JSONDecodeError:
            values.append(x)
            continue

        try:
            value = x['#text']
        except KeyError:
            # not an elementtree type data value
            values.append(x)
            continue

        if not x.get('attrib'):
            values.append(x['#text'])
        else:
            suppress = False
            for attrib, attval in x['attrib'].items():
                if attval in suppress_attribs.get(attrib, []):
                    suppress = True
                    break
                if not suppress:
                    values.append(x['text'])

    return values

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

    spark.udf.register("get_values", get_values_from_text_attrib, StringType())
    
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

    # # these fields directly map to a field of the same name
    direct_fields = [
        'contributor',
        'creator', 
        'extent',
        'language',
        'publisher',
        'provenance'
    ]

    # tuple of format: fields to combine from source, field to map to in destination
    combine_fields = [
        (['abstract', 'description', 'tableOfContents'], 'description'),
        (['bibliographicCitation', 'identifier'], 'identifier'),
        (['accessRights', 'rights'], 'rights')
    ]

    direct_fields = [f for f in direct_fields if f in oac_source.columns]
    if direct_fields:
        direct_fields.append(calisphere_id)
        oac_mapped = oac_source.select(direct_fields)
    

    for source_fields, destination_field in combine_fields:
        source_fields = [f for f in source_fields if f in oac_source.columns]
        if source_fields:
            arr_source_fields = [f for f in source_fields if get_dtype(oac_source, f).startswith('array')]
            str_source_fields = [f for f in source_fields if get_dtype(oac_source, f) == 'string']
            print(source_fields)
            print(arr_source_fields)
            print(str_source_fields)
            if len(arr_source_fields) == len(source_fields):
                # coalesce allows us to concat even if one of source_fields is NULL
                # https://stackoverflow.com/questions/37284077/combine-pyspark-dataframe-arraytype-fields-into-single-arraytype-field
                coalesce_f = [coalesce(col(sf), array()) for sf in source_fields]
                combine_df = oac_source.select(calisphere_id,
                    concat(*coalesce_f).alias(destination_field))
            elif len(str_source_fields) == len(source_fields):
                combine_df = oac_source.select(calisphere_id, 
                    concat(*source_fields).alias(destination_field))

            expr_df = (combine_df
                .selectExpr(f'`{calisphere_id}`', f'get_values(`{destination_field}`)')
                .withColumnRenamed(f'get_values({destination_field})', destination_field))
            oac_mapped = oac_mapped.join(expr_df, 'calisphere-id')


    oac_mapped.printSchema()
    oac_mapped.show(10, truncate=True)

    # exclude_subfields = [
    #     ('date', 'dcterms:dateCopyrighted', 'date'),
    #     ('format', 'x', 'format'),
    #     ('title', 'alternative', 'title'),
    #     ('type', 'genreform', 'type'),
    #     ('subject', 'series', 'subject')
    # ]

    # specific_subfield = [
    #     ('date', 'dcterms:dateCopyrighted', 'copyrightDate'),
    #     # ('coverage', '') coverage is complicated and not in this sample data - maps to spatial and temporal
    #     ('title', 'alternative', 'alternativeTitle'),
    #     ('type', 'genreform', 'genre')
    # ]

    # add_fields = [
    #     ([{"name": "California"}], 'stateLocatedIn')
    # ]

    # # if has_column(oac_source, 'title'):
    # #     if get_dtype(oac_source, 'title') == 'string':
    # #         oac_mapped = oac_mapped.join(oac_source.select('title', 'calisphere-id'), 'calisphere-id')
    # #         oac_mapped.printSchema()
    # #         oac_mapped.show(10, truncate=True)
    # #     else:
    # #         print('oac mapper not yet setup to handle non-string titles')

    # # if has_column(oac_source, 'rights') or has_column(oac_source, 'accessRights'):

