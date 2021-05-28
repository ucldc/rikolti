import sys
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 


def main(database, table): 
	original_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
	nuxeo_df = original_DyF.toDF().distinct()

	# filter null values, report out null_df
	null_type_df = nuxeo_df.filter(col('type').isNull())
	nuxeo_df = nuxeo_df.filter(col('type').isNotNull())

	# filter out objects with types we don't know how to process
	good_type = ["SampleCustomPicture", "CustomAudio", "CustomVideo", "CustomFile", "Organization"] 
	bad_type = nuxeo_df.filter(~col('type').isin(good_type))
	nuxeo_df = nuxeo_df.filter(col('type').isin(good_type))

	# filter out objects with no properties
	null_properties_df = nuxeo_df.filter(col('properties').isNull())
	nuxeo_df = nuxeo_df.filter(col('properties').isNotNull())

	# filter out objects with no file content
	null_file_df = nuxeo_df.filter(col('properties.file:content').isNull())
	nuxeo_df = nuxeo_df.filter(col('properties.file:content').isNotNull())

	# filter out objects with empty_picture placeholder
	# file is not guaranteed, so first check the column exists at all
	file_content_schema = nuxeo_df.select('properties.file:content').schema[0].dataType.fieldNames()
	if 'file' in file_content_schema:
		empty_picture_df = nuxeo_df.filter(col('properties.file:content.file')=='empty_picture.png')
		nuxeo_df = nuxeo_df.filter(col('properties.file:content.file')!='empty_picture.png')

	non_video_df = None
	mp4_media = None
	metadata_properties_schema = nuxeo_df.select('properties').schema[0].dataType.fieldNames()
	if "vid:transcodedVideos" in metadata_properties_schema:
		video_df = (nuxeo_df
			.filter(col('type').isin(['CustomVideo']))
			.filter(size('properties.vid:transcodedVideos')>0)
			.select(
				'calisphere-id', 
				explode('properties.vid:transcodedVideos').alias('transcodedVideos')
			)
			.filter(col('transcodedVideos.content.mime-type')==lit('video/mp4'))
			.groupBy('calisphere-id')
			.agg(collect_list('transcodedVideos').alias('transcodedVideos'))
			.select(
				'calisphere-id', 
				col('transcodedVideos').getItem(0).alias('transcodedVideos')
			)
		)
		info = {'data', 'mime-type', 'name'}
		transcodedVideos_content_schema = video_df.select('transcodedVideos.content').schema[0].dataType.fieldNames()
		available_fields = set(transcodedVideos_content_schema).intersection(info)
		available_fields = ['transcodedVideos.content.'+f for f in available_fields]
		available_fields.append('calisphere-id')
		mp4_media = (video_df
			.select(available_fields)
			.withColumn('url', regexp_replace('data', '/nuxeo/', '/Nuxeo/'))
			.withColumn('filename', col('name'))
			.drop('data', 'name')
		)
		non_video_df = nuxeo_df.join(mp4_media, "calisphere-id", "left_anti")

	if non_video_df is None:
		non_video_df = nuxeo_df

	info = {'data', 'mime-type', 'name'}
	available_fields = set(file_content_schema).intersection(info)
	available_fields = ['properties.file:content.'+f for f in available_fields]
	available_fields.append('calisphere-id')
	media_df = (non_video_df
		.select(available_fields)
		.withColumn('url', regexp_replace('data', '/nuxeo/', '/Nuxeo/'))
		.withColumn('filename', col('name'))
		.drop('data', 'name')
	)

	if mp4_media:
		new = media_df.union(mp4_media)
	else:
		new = media_df

	nullFilename = new.filter(col('filename').isNull())
	# old Deep Harvester tried a fall back to 
	# nuxeo_src['properties']['file:filename'] in this case before throwing an 
	# error if no ['properties']['file:filename']. I've yet to encounter a record 
	# with `file:filename` in the schema though

	new = new.filter(col('filename').isNotNull())
	new_struct = new.withColumn('contentFile', struct(col('mime-type'), col('url'), col('filename')))
	new_struct = new_struct.drop('mime-type', 'url', 'filename')

	image_df = nuxeo_df.filter(col('type') == lit('SampleCustomPicture'))
	image_df = image_df.filter(col('properties.file:content.data').isNotNull())
	image_df = image_df.select('calisphere-id', 'uid')
	image_df = (image_df.withColumn('thumbnail', concat(
			lit("https://nuxeo.cdlib.org/Nuxeo/nxpicsfile/default/"), 
			col('uid'), 
			lit("/Medium:content")
		)
	))

	new_struct = new_struct.join(image_df, 'calisphere-id')
	new_struct.show()

	# convert to glue dynamic frame
	transformed_DyF = DynamicFrame.fromDF(new_struct, glueContext, "transformed_DyF")
	transformed_DyF.show()

	# write transformed data to target
	now = datetime.now()
	collection_id = table
	dt_string = now.strftime("%Y-%m-%d")
	path = "s3://ucldc-ingest/glue-test-data-target/mapped/media-{}".format(dt_string)

	partition_keys = ['uid'] 
	glueContext.write_dynamic_frame.from_options(
	   frame = transformed_DyF,
	   connection_type = "s3",
	   connection_options = {"path": path, "partitionKeys": partition_keys},
	   format = "json")



if __name__ == '__main__':

	# Create a Glue context
	glueContext = GlueContext(SparkContext.getOrCreate()) 

	spark = glueContext.spark_session # SparkSession provided with GlueContext. Pass this around at runtime rather than instantiating within every python class

	database = "pachamama-demo"
	# table = "27414" # pdfs
	# table = "26710" # videos
	table = "9513"  # images

	sys.exit(main(database, table))




