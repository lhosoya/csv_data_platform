import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from airflow import DAG
from airflow.decorators import dag, task

@task(task_id='abc')
def my_task():
    print("Hello, World!")
    
    
@task(task_id='run_simple_spark')
def run_spark_job():
    print("Started")
    spark = (
	SparkSession.builder.appName("PySparkTest")
	.config("spark.hadoop.fs.s3a.access.key", 'minio')
	.config("spark.hadoop.fs.s3a.secret.key", 'minio123')
	.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") #variable depending on the minio container IP
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
    .master("local[*]")
	.getOrCreate()
    )
    print(spark)
    print("Reading data")
    df = spark.read.csv(
	"s3a://landing/sample_events.csv",
	header=True,
	inferSchema=True
    )
    print(df.count())
    print(df.show(5,False))
    
    
    print("read!")
    df.write.format("delta").mode("overwrite").saveAsTable("events_delta")
    print('written to delta table')
    spark.stop()


with DAG(
    dag_id="spark_dag_write",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
):
    
    t = my_task()
    t1 = run_spark_job()

    t >> t1