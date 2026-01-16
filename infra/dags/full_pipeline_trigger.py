from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


with DAG(
    dag_id='kafka_to_lakehouse_full_pipeline_trigger',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False
) as dag:

    kafka = TriggerDagRunOperator(
        task_id='kafka',
        trigger_dag_id='producer_dag',
        conf={"event_no": "1"},
        wait_for_completion=True
            
    )
    
    kafka_to_bronze = TriggerDagRunOperator(
        task_id='kafka_to_bronze',
        trigger_dag_id='lakehouse_kafka_to_bronze',
        conf={"table_path": "s3a://lakehouse/bronze/final_events"},
        poke_interval=5,
        wait_for_completion=True
    )
    
    bronze_to_silver = TriggerDagRunOperator(
        task_id='bronze_to_silver',
        trigger_dag_id='lakehouse_bronze_to_silver',
        conf={"bronze_path": "s3a://lakehouse/bronze/final_events", "silver_path": "s3a://lakehouse/silver/final_events"},
        poke_interval=5,
        wait_for_completion=True
    )
    
    kafka >> kafka_to_bronze >> bronze_to_silver