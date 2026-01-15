#%%
# from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.functions import from_json, col,get_json_object,to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F
#import minio_secrets

# dNZdDeDX645GJVlBtMZU
# 40d5JcF0ZvmK2AbS1S2Cv5WNeNHGBaupN31AL5gG
# %%
spark = (
	SparkSession.builder.appName("PySparkTest")
	.config("spark.hadoop.fs.s3a.access.key", 'minio')
	.config("spark.hadoop.fs.s3a.secret.key", 'minio123')
	.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
	.config(
		"spark.jars.packages",
		"org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.kafka:kafka-clients:3.4.0"
	)
	.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	.config("spark.hadoop.fs.s3a.path.style.access", "true")
	.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
	.config("spark.sql.warehouse.dir", "s3a://lakehouse")
	.config("spark.sql.session.timeZone", "UTC")
	.getOrCreate()
)
# %%

df = spark.read \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9094") \
	.option("subscribe", "stream1") \
	.option("startingOffsets", "earliest") \
	.load()
#%%
df.show(5,False)
# %%
df.count()
# Create a Delta table in the warehouse

# %%


df_json = df.selectExpr("CAST(KEY AS STRING) as key", "CAST(value AS STRING) as value",
                        'topic', 'partition', 'offset', 'timestamp as kafka_timestamp')


df_json.show(5,False)

# Dynamically infer schema from the 'value' column
sample_json = df_json.select('value').rdd.map(lambda row: row['value']).filter(lambda x: x is not None).take(100)
if sample_json:
	from pyspark.sql.types import StructType
	import json
	# Infer schema from sample JSONs
	merged_keys = set()
	for s in sample_json:
		try:
			merged_keys.update(json.loads(s).keys())
		except Exception:
			pass
	inferred_schema = StructType([StructField(k, StringType(), True) for k in merged_keys])
	# Parse the value column
	df_parsed = df_json.withColumn('parsed', from_json(col('value'), inferred_schema))
	# Flatten the parsed columns
	for k in merged_keys:
		df_parsed = df_parsed.withColumn(k, col('parsed')[k])
	# Drop the intermediate 'parsed' column if desired
	df_parsed = df_parsed.drop('parsed')
	df_parsed.show(5, False)
	df_parsed.printSchema()
else:
	print("No valid JSON samples found in 'value' column.")
 
#%%
# %%
# Convert event_ts to UTC timestamp, then extract date and hour
df_parsed = df_parsed.withColumn("event_ts_utc", F.to_timestamp(col("event_ts")))
df_parsed = df_parsed.withColumn("event_date", F.date_format(col("event_ts_utc"), "yyyy-MM-dd"))
df_parsed = df_parsed.withColumn("event_hour", F.date_format(col("event_ts_utc"), "HH"))
df_parsed = df_parsed.drop("value")
# %%
df_parsed.printSchema()
# %%
df_parsed.show(5,False)
# %%
# Order by event_ts ascending
#df_parsed = df_parsed.orderBy(col("event_ts").asc())

# Save as Delta table partitioned by event_date and event_hour, with ZSTD compression
df_parsed.write \
	.format("delta") \
	.mode("append") \
    .partitionBy("event_date",'event_hour') \
	.option("compression", "zstd") \
    .save("s3a://lakehouse/test_events_kafka2")\
    
    # .save("s3a://lakehouse/test_events_kafka10")\
    
	
	#.option("compression", "zstd") \
	

# If you want to optimize the Delta table for event_ts (requires Delta Lake 2.0+ and Spark SQL)
# try:
# 	spark.sql("OPTIMIZE kafka_events_delta ZORDER BY (event_ts)")
# except Exception as e:
# 	print(f"Delta OPTIMIZE ZORDER failed: {e}")
# %%
df_parsed.show(5,False)
# %%


CREATE VIEW IF NOT EXISTS v_test_events_kafka2 as 
select * from deltaLakeS3('http://minio:9000/lakehouse/test_events_kafka2', 'minio', 'minio123');

select * from v_test_events_kafka2;