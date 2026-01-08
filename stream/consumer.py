# %%
from pyspark.sql import SparkSession
import os
import sys
# Read from Kafka topic
df = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9094") \
	.option("subscribe", "your_topic") \
	.option("startingOffsets", "latest") \
	.load()

# Convert key and value from binary to string
from pyspark.sql.functions import col
messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

# Output to console
query = messages.writeStream \
	.outputMode("append") \
	.format("console") \
	.option("truncate", False) \
	.start()

query.awaitTermination()
sys.path.append(os.path.abspath("../secrets"))
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
