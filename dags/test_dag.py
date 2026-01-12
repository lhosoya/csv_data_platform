import datetime

from airflow import DAG
from airflow.decorators import dag, task

@task(task_id='abc')
def my_task():
    print("Hello, World!")


with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
):
    
    t = my_task()

    t