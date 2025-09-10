from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Hello from Airflow!")

dag = DAG(
    'my_dag',
    start_date=datetime(2025, 1, 18),
    schedule_interval='@daily'
)

task = PythonOperator(
    task_id='print_hello',
    python_callable=my_task,
    dag=dag
)

# Manually run the task
task.execute(context={})
