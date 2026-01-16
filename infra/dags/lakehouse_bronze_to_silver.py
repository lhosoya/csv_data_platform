import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

import os
import sys
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window
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

@task(task_id='bronze_to_silver')
def pipeline(bronze_path = 's3a://lakehouse/bronze/final_events', silver_path = 's3a://lakehouse/silver/final_events'):
    spark = spark_custom.getOrCreate()
    table_exists_flag = table_exists(spark, silver_path)
    if table_exists_flag:
        print("Silver table exists.")
        max_load_ts = df_silver.agg(F.max("load_timestamp")).collect()[0][0]
        df_silver = spark.read.format("delta").load(bronze_path) \
            .filter(col("sys_load_timestamp") >= F.lit(max_load_ts))
    else:
        df_silver = spark.read \
            .format("delta") \
            .load(bronze_path)
            
    # window_spec = Window.partitionBy("event_id").orderBy(F.col("event_ts_utc").desc())
    # df_silver = df_silver.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")

    df_silver = df_silver.withColumnRenamed("pipe_timestamp_utc", "load_timestamp")
    df_silver = df_silver.withColumnRenamed("event_ts_utc", "event_timestamp_utc")
    df_silver = df_silver.drop("partition", "offset","topic", "key", "value")
    print(df_silver.count())
    
    if table_exists_flag:
        # Table exists, perform upsert (merge)
        from delta.tables import DeltaTable
        delta_silver = DeltaTable.forPath(spark, silver_path)
        target_df = delta_silver.toDF()
        # Find records to insert (not present in target)
        insert_candidates = df_silver.alias("source_ins").join(
            target_df.alias("target"),
            (F.col("source_ins.event_id") == F.col("target.event_id")) &
            (F.col("source_ins.event_date") == F.col("target.event_date")) &
            (F.col("source_ins.event_hour") == F.col("target.event_hour")),
            how="left_anti"
        )
    
        df_silver_upd= df_silver.selectExpr("user_id as user_id_upd","event_id as event_id_upd", "event_date as event_date_upd", "event_hour as event_hour_upd","load_timestamp as load_timestamp_upd")
        # Find records to update (present in both
        
        update_candidates = df_silver_upd.alias("source_upd").join(
            target_df.alias("target"),
            (F.col("source_upd.event_id_upd") == F.col("target.event_id")) &
            (F.col("source_upd.event_date_upd") == F.col("target.event_date")) &
            (F.col("source_upd.event_hour_upd") == F.col("target.event_hour")),
            how="inner"
        )
        #update_candidates.show(5,False)
        # Alias all columns in update_candidates with 'to_update_' prefix
        print("Modified users:")
        to_update_users = insert_candidates.select("source_ins.user_id").distinct().union(update_candidates.select("source_upd.user_id_upd").distinct()).distinct()
        print(to_update_users.show(truncate=False))
        print("Distinct users to insert:")
        print(insert_candidates.select("source_ins.user_id", "source_ins.event_id").distinct().show(truncate=False))
        print("Distinct users to update:")
        print(update_candidates.select("source_upd.user_id_upd", "source_upd.event_id_upd").distinct().show(truncate=False))
        # Upsert only insert_candidates, update only update_candidates
        # Insert new records
        update_candidates.show(5,False)
    
        print("Inserting new records...")
        delta_silver.alias("target").merge(
            insert_candidates.alias("source_ins"),
            "target.event_id = source_ins.event_id AND target.event_date = source_ins.event_date AND target.event_hour = source_ins.event_hour"
        ).whenNotMatchedInsertAll().execute()
        #Update existing records
        print("Updating existing records...")
        delta_silver.alias("target").merge(
            update_candidates.alias("source_upd"),
            "target.event_id = source_upd.event_id_upd AND target.event_date = source_upd.event_date_upd AND target.event_hour = source_upd.event_hour_upd"
        ).whenMatchedUpdate(
            condition="source_upd.load_timestamp_upd > target.load_timestamp",
            set={"target.load_timestamp": "source_upd.load_timestamp_upd"}
        ).execute()
    else:
        # Table does not exist, create it with deduplicated DataFrame schema
        window_spec_create = Window.partitionBy("event_id").orderBy(F.col("event_timestamp_utc").desc())
        df_silver_create = df_silver.withColumn("row_number", F.row_number().over(window_spec_create)).filter(F.col("row_number") == 1).drop("row_number")
        df_silver_create = df_silver_create.orderBy(F.col("event_timestamp_utc").asc())
        df_silver_create.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("event_date", "event_hour") \
            .option("compression", "zstd") \
            .option("mergeSchema", "true") \
            .save(silver_path)
    
@task(task_id='optimize')
def optimize_silver(silver_path = 's3a://lakehouse/silver/final_events'):
    spark = spark_custom.getOrCreate()
    try:
        spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (event_timestamp_utc, user_id)")
        print("Successfully optimized silver table.")
    except Exception as e:
        print(f"Delta OPTIMIZE ZORDER failed: {e}")


with DAG(
    dag_id="lakehouse_bronze_to_silver",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    params={
        "bronze_path": Param(default='s3a://lakehouse/bronze/final_events', type="string"),
        "silver_path": Param(default='s3a://lakehouse/silver/final_events', type="string")
    }
) as dag:
    bronze_path = '{{ params.bronze_path }}'
    silver_path = '{{ params.silver_path }}'
    to_bronze = pipeline(bronze_path, silver_path)
    optimize = optimize_silver(silver_path)
    
    create_table = ClickHouseOperator(task_id='clickhouse_test_query',
        clickhouse_conn_id='clickhouse_connection',
        database='default',
        sql="""drop view if exists v_silver_final_events PARALLEL WITH
                CREATE VIEW IF NOT EXISTS v_silver_final_events as 
                select * from deltaLakeS3('http://minio:9000/lakehouse/silver/final_events', 'minio', 'minio123');
            """,
        )
    
    to_bronze >> optimize >> create_table