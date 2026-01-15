#%%
from pyspark.sql import SparkSession
import os
import sys
#sys.path.append(os.path.abspath("../secrets"))
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
    .config("spark.sql.warehouse.dir", "s3a://raw/delta_warehouse")
	.getOrCreate()
)
# %%
df = spark.read.csv(
	"s3a://landing/sample_events.csv",
	header=True,
	inferSchema=True
)
# %%
df.count()
# %%

from pyspark.sql.functions import to_json, struct, col, concat_ws

# Add UDF for compression + base64 encoding
import zlib
import base64 as pybase64
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def compress_and_encode(s):
	if s is None:
		return None
	compressed = zlib.compress(s.encode())
	return pybase64.b64encode(compressed).decode()

compress_and_encode_udf = udf(compress_and_encode, StringType())

kafka_df = df.withColumn(
	"key",
	compress_and_encode_udf(concat_ws("|", col("event_id"), col("user_id"), col("event_type"), col("event_ts"), col("plan_id"), col("amount"), col("source")))
).withColumn(
	"value",
	to_json(struct(
		"event_id", "user_id", "event_type", "event_ts", "plan_id", "amount", "source"
	))
)
#%%
kafka_df.select("key", "value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("topic", "stream1") \
    .save()
#%%
#spark.stop()

df.printSchema()
# %%
