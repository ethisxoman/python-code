# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime

# # ---------------------
# # Define your task functions
# # ---------------------
# def extract_data():
#     print("Extracting data...")
#     # Example: fetch from DB or API
#     return {"user_id": 1, "name": "Aqsa"}

# def transform_data():
#     print("Transforming data...")
#     # Example: clean / process data

# def load_data():
#     print("Loading data...")
#     # Example: write to reporting DB or save to file

# # ---------------------
# # DAG definition
# # ---------------------
# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2025, 8, 18),  # start date of DAG
#     "retries": 1,
# }

# with DAG(
#     dag_id="user_report_pipeline",   # <-- this must match what you test
#     default_args=default_args,
#     schedule_interval="@daily",      
#     catchup=False,
#     tags=["reporting", "user"],
# ) as dag:

#     task_extract = PythonOperator(
#         task_id="extract_data",
#         python_callable=extract_data,
#     )

#     task_transform = PythonOperator(
#         task_id="transform_data",
#         python_callable=transform_data,
#     )

#     task_load = PythonOperator(
#         task_id="load_data",
#         python_callable=load_data,
#     )

#     # Define pipeline order
#     task_extract >> task_transform >> task_load
