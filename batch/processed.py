#%%
from pyspark.sql import SparkSession
import os
import sys
#sys.path.append(os.path.abspath("../secrets"))
#import minio_secrets

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

#%%
# %%
df = spark.read \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9094") \
	.option("subscribe", "stream1") \
	.option("startingOffsets", "earliest") \
	.load()
# %%
df.count()
# %%
# Normalize JSON in value column
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define your JSON schema here. Example:
json_schema = StructType([
	StructField("field1", StringType(), True),
	StructField("field2", IntegerType(), True),
	StructField("field3", DoubleType(), True)
	# Add more fields as needed
])

df_json = df.selectExpr("CAST(value AS STRING) as value") \
	.withColumn("jsonData", from_json(col("value"), json_schema))

df_normalized = df_json.select("jsonData.*")
df_normalized.show(5, False)
# %%
