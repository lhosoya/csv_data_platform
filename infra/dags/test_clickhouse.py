

from airflow import DAG
from airflow.decorators import dag, task
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator






@task(task_id='abc')
def my_task():
    print("Hello, World!")
    
@task(task_id='run_simple_spark')
def run_spark_job():
    print('job')
    



with DAG(
    dag_id="clickhouse_test_dag",
    start_date=None,
    schedule=None,
) as dag:
    x = ClickHouseOperator(task_id='clickhouse_test_query',
        clickhouse_conn_id='clickhouse_connection',
        database='default',
        sql="""create table testme123 as SELECT *
            FROM deltaLake(
                'http://minio:9000/lakehouse/events_delta',
                'minio',
                'minio123'
            )""",
        )
    
    t = my_task()
    t1 = run_spark_job()

    t >> t1 >> x