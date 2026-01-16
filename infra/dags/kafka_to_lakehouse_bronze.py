from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


import os
import sys
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType
import datetime
import pyspark.sql.functions as F
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from custom_spark import spark_custom

def table_exists(spark, table_path):
    print("Check table")
    try:
        spark.read.format("delta").load(table_path)
        return True
    except Exception:
        return False

def get_max_ts(df_bronze):
    print("get_max_ts called")
    max_ts_df = df_bronze.select(F.max("sys_load_timestamp").alias("max_sys_load_timestamp"))
    max_ts_df = max_ts_df.withColumn(
		"ts_minus_10min",
		F.expr("CAST(CAST(max_sys_load_timestamp AS TIMESTAMP) - INTERVAL 10 MINUTES AS TIMESTAMP)")
	).withColumn(
		"ts_minus_10min_trunc",
		F.date_trunc("second", F.col("ts_minus_10min"))
	).withColumn(
		"ts_minus_10min_unix_ms",
		(F.unix_timestamp(F.col("ts_minus_10min_trunc")) * 1000).cast("long")
	)
    row = max_ts_df.collect()[0]
    print(row)
    start_unix = int(row.ts_minus_10min_unix_ms) if row.ts_minus_10min_unix_ms is not None else None
    return start_unix

@task(task_id='kafka_to_bronze')
def pipeline(bronze_path = 's3a://lakehouse/bronze/final_events'):
    spark = spark_custom.getOrCreate()
    table_exists_flag = table_exists(spark, bronze_path)
    if table_exists_flag:
        print("Bronze table exists.")
        df_bronze = spark.read.format("delta").load(bronze_path)
        start_unix = get_max_ts(df_bronze)
    else:
        start_unix=0
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stream") \
        .option("startingTimestamp", start_unix) \
        .load()
    df = df.withColumn("kafka_timestamp_utc", F.to_utc_timestamp(F.col("timestamp"), "America/Sao_Paulo"))
    df_json = df.selectExpr("CAST(KEY AS STRING) as key", "CAST(value AS STRING) as value",
                        'topic', 'partition', 'offset', 'timestamp as sys_load_timestamp')
    # Dynamically infer schema from the 'value' column
    sample_json = df_json.select('value').rdd.map(lambda row: row['value']).filter(lambda x: x is not None).take(1000)
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
        
    df_parsed = df_parsed.withColumn("event_ts_utc", F.to_timestamp(col("event_ts")))
    df_parsed = df_parsed.withColumn("event_date", F.date_format(col("event_ts_utc"), "yyyy-MM-dd"))
    df_parsed = df_parsed.withColumn("event_hour", F.date_format(col("event_ts_utc"), "HH"))
    df_parsed = df_parsed.withColumn("load_date", F.date_format(col("sys_load_timestamp"), "yyyy-MM-dd"))
    df_parsed = df_parsed.withColumn("pipe_timestamp_utc", F.current_timestamp()) 
    df_parsed = df_parsed.orderBy(col("load_date").asc())
    print(df_parsed.printSchema())
    
    df_parsed.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("load_date") \
        .option("compression", "zstd") \
        .option("mergeSchema", "true") \
        .save(f"{bronze_path}")

@task(task_id='optimize')
def optimize_bronze(bronze_path = 's3a://lakehouse/bronze/final_events'):
    spark = spark_custom.getOrCreate()
    try:
        spark.sql(f"OPTIMIZE delta.`{bronze_path}` ZORDER BY (event_ts_utc, user_id)")
        print("Successfully optimized bronze table.")
    except Exception as e:
        print(f"Delta OPTIMIZE ZORDER failed: {e}")


with DAG(
    dag_id="lakehouse_kafka_to_bronze",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    params={
        "table_path": Param(default='s3a://lakehouse/bronze/final_events', type="string")
    }
) as dag:
    table_path = '{{ params.table_path }}'
    to_bronze = pipeline(table_path)
    optimize = optimize_bronze(table_path)
    
    create_table = ClickHouseOperator(task_id='clickhouse_test_query',
        clickhouse_conn_id='clickhouse_connection',
        database='default',
        sql="""drop view if exists v_bronze_final_events PARALLEL WITH
                CREATE VIEW IF NOT EXISTS v_bronze_final_events as 
                select * from deltaLakeS3('http://minio:9000/lakehouse/bronze/final_events', 'minio', 'minio123');
            """,
        )
    
    to_bronze >> optimize >> create_table