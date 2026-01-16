import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from airflow import DAG
from airflow.decorators import dag, task


import zlib
import base64 as pybase64
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from custom_spark import spark_custom
from pyspark.sql.functions import to_json, struct, col, concat_ws
from airflow.models.param import Param


# Add UDF for compression + base64 encoding
@udf(StringType())
def compress_and_encode(s):
	if s is None:
		return None
	compressed = zlib.compress(s.encode())
	return pybase64.b64encode(compressed).decode()

def prepare_data(df):
	return df.withColumn(
		"key",
		compress_and_encode(concat_ws("|", col("event_id"), col("user_id"), col("event_type"), col("event_ts"), col("plan_id"), col("amount"), col("source")))
	).withColumn(
		"value",
		to_json(struct(
			"event_id", "user_id", "event_type", "event_ts", "plan_id", "amount", "source"
		))
	)

@task(task_id='produce_data')
def read_csv_data(event_no):
    spark= spark_custom.getOrCreate()
    print("READING!")
    df = spark.read.csv(
        f"s3a://landing/sample_events_{event_no}.csv",
        header=True,
        inferSchema=True
        )
    print("Read complete")
    df.count()
    
    kafka_df = prepare_data(df)
    
    kafka_df.select("key", "value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "stream") \
    .save()
    print("Write complete")
    spark.stop()


with DAG(
    dag_id="producer_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    params={
        "event_no": Param(default='1', type="string")
    },
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    event_no = '{{ params.event_no }}'
    produce = read_csv_data(event_no)
    
    produce