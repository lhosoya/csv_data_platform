#%%
# from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import os
import sys
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
		"org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0"
	)
	.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	.config("spark.hadoop.fs.s3a.path.style.access", "true")
	.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
df.write.format("delta").save("s3a://raw/sample_events_delta")
# %%
