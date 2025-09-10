import math
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """
    #---------------WALLET WITHDRAW FUNDS----------------#
SELECT
u.id AS user_id,
      
w.amount AS withdraw_funds_amount, w.currency AS withdraw_funds_currency, 
w.base_currency AS withdraw_funds_base_currency, w.exchange_rate AS withdraw_funds_exchange_rate, 
w.converted_amount AS withdraw_funds_converted_amount, w.remaining_balance AS withdraw_funds_remaining_balance, 
w.bank AS withdraw_funds_bank_id, 
    case when 	 w.payment_type = 1 then "Bank" 
    when w.payment_type = 2 then "Credit/Debit" 
    else null end AS withdraw_funds_payment_type, 

w.is_processed AS withdraw_funds_status, w.created_at AS withdraw_funds_created_at, 
w.updated_at AS withdraw_funds_updated_at, 

ub.bank_name, ub.ac_name AS account_name, ub.ac_no AS account_num, ub.country_id, 
ub.branch, ub.iban, ub.home_address, ub.swift_code

FROM ethisx_accounts.users u 

INNER JOIN ethisx_wallet_live.withdraw_funds AS w 
	ON u.id = w.user_id

LEFT JOIN ethisxadmin.user_bank_infos AS ub
	ON u.id = ub.user_id 
order by u.id

"""

#extract and load
def extract_and_load_func(**dictionary):
    print("Connecting to SOURCE database...")
    #extract
    try:
        src_conn = get_airflow_connection(source_db)
        src_cur = src_conn.cursor()
        print("Successfully connected to SOURCE database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to source DB: {e}")
        return

    print("Executing source query...")
    try:
        src_cur.execute(query)
        rows = src_cur.fetchall()
        columns = [desc[0] for desc in src_cur.description]
        print(f"Query executed. Rows fetched: {len(rows)}")
        
    except Exception as e:
        print(f"[ERROR] Failed executing query: {e}")
        src_conn.close()
        return

    src_conn.close()
    print("Source DB connection closed.")

    if not rows:
        print("No rows found in source database.")
        return

    df = pd.DataFrame(rows, columns=columns)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

    #load
    print("Connecting to REPORTING database...")
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        print("Successfully connected to REPORTING database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to reporting DB: {e}")
        return
    
    table_name = "ethisx_reporting.wallet_withdraw_funds"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    withdraw_funds_amount VARCHAR(255),
    withdraw_funds_currency VARCHAR(3),
    withdraw_funds_base_currency VARCHAR(3),
    withdraw_funds_exchange_rate DECIMAL(16,8),
    withdraw_funds_converted_amount DECIMAL(16,2),
    withdraw_funds_remaining_balance VARCHAR(255),
    withdraw_funds_bank_id VARCHAR(255),
    withdraw_funds_payment_type VARCHAR(50),
    withdraw_funds_status [2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - Query executed. Rows fetched: 9
[2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - Source DB connection closed.
[2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - DataFrame created with 9 rows and 20 columns.
[2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - Connecting to REPORTING database...
[2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - Successfully connected to REPORTING database.
[2025-09-03, 11:47:46 PKT] {logging_mixin.py:188} INFO - Ensuring table exists: ethisx_reporting.wallet_withdraw_funds
[2025-09-03, 11:47:47 PKT] {logging_mixin.py:188} INFO - Truncating table ethisx_reporting.wallet_withdraw_funds before inserting fresh data...
[2025-09-03, 11:47:47 PKT] {taskinstance.py:441} ▼ Post task execution logs
[2025-09-03, 11:47:47 PKT] {taskinstance.py:2905} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/airflow/models/taskinstance.py", line 432, in _execute_callable
    return execute_callable(context=context, **execute_callable_kwargs)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
    return func(self, *args, **kwargs)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/airflow/operators/python.py", line 235, in execute
    return_value = self.execute_callable()
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/airflow/operators/python.py", line 252, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/DAGS/wallet_withdraw_funds_airflow.py", line 161, in extract_and_load_func
    dest_cur.execute(insertion, list(dict_row.values()))
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/cursors.py", line 153, in execute
    result = self._query(query)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/cursors.py", line 322, in _query
    conn.query(q)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/connections.py", line 575, in query
    self._affected_rows = self._read_query_result(unbuffered=unbuffered)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/connections.py", line 826, in _read_query_result
    result.read()
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/connections.py", line 1203, in read
    first_packet = self.connection._read_packet()
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/connections.py", line 782, in _read_packet
    packet.raise_for_error()
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/protocol.py", line 219, in raise_for_error
    err.raise_mysql_exception(self._data)
  File "/home/abdullah/Desktop/Ethisx_Reporting/ssquare/reporting_env/venv/lib/python3.10/site-packages/pymysql/err.py", line 150, in raise_mysql_exception
    raise errorclass(errno, errval)
pymysql.err.DataError: (1366, "Incorrect integer value: 'APPROVED' for column 'withdraw_funds_status' at row 1")
[2025-09-03, 11:47:47 PKT] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=wallet_withdraw_funds_airflow, task_id=extract_and_load, run_id=manual__2025-09-03T06:47:39.114586+00:00, execution_date=20250903T064739, start_date=20250903T064745, end_date=20250903T064747
[2025-09-03, 11:47:47 PKT] {standard_task_runner.py:110} ERROR - Failed to execute job 2316 for task extract_and_load ((1366, "Incorrect integer value: 'APPROVED' for column 'withdraw_funds_status' at row 1"); 35905)
[2025-09-03, 11:47:47 PKT] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2025-09-03, 11:47:47 PKT] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2025-09-03, 11:47:47 PKT] {local_task_job_runner.py:222} ▲▲▲ Log group end,
    withdraw_funds_created_at DATETIME,
    withdraw_funds_updated_at DATETIME,
    
    bank_name VARCHAR(255),
    account_name VARCHAR(255),
    account_num VARCHAR(255),
    country_id BIGINT,
    branch VARCHAR(255),
    iban VARCHAR(255),
    home_address TEXT,
    swift_code VARCHAR(255)

    -- UNIQUE KEY uniq_user_withdraw (id)
);
    """
    dest_cur.execute(create_table)

    # for _, row in df.reset_index(drop=True).iterrows():
    #     dict_row = row.to_dict()

    #     #replace NaN with None for MySQL
    #     for k, v in dict_row.items():
    #         if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
    #             dict_row[k] = None

    #     columns = list(dict_row.keys())
    #     col_names = ", ".join([f"`{col}`" for col in columns])
    #     placeholders = ", ".join(["%s"] * len(columns))

    #     insertion = f"""
    #     INSERT IGNORE INTO {table_name} ({col_names})
    #     VALUES ({placeholders})
    #     """  
  
    #     dest_cur.execute(insertion, list(dict_row.values()))
    dest_cur.execute(f"""
        ALTER TABLE {table_name} 
        MODIFY COLUMN withdraw_funds_payment_type VARCHAR(50)
    """)

    #Truncate table each run (optional, but ensures no missing rows)
    print(f"Truncating table {table_name} before inserting fresh data...")
    dest_cur.execute(f"TRUNCATE TABLE {table_name}")

    for _, row in df.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()

        # replace NaN with None for MySQL
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        columns = list(dict_row.keys())
        col_names = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        insertion = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        """  

        dest_cur.execute(insertion, list(dict_row.values()))


    dest_conn.commit()
    dest_cur.close()
    dest_conn.close()
    print(f"Inserted {len(df)} new rows into {table_name}.")
    print("Reporting DB connection closed.")

#DAG
with DAG(
    dag_id="wallet_withdraw_funds_airflow",
    start_date=days_ago(1),      #starts yesterday, so scheduler will pick it up
    schedule_interval="0 */6 * * *",   
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
    )