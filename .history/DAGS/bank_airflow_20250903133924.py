import math
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """SELECT DISTINCT
    -- u.id AS user_id, 

    #---------------ADMIN----------------#
    ub.bank_name, ub.ac_name, ub.ac_no, ub.branch, ub.swift_code, ub.iban, ub.home_address

FROM ethisx_accounts.users AS u
LEFT JOIN ethisxadmin.user_bank_infos AS ub
	ON u.id = ub.user_id    
WHERE u.id IS NOT NULL;
"""

#EXTRACT+LOAD
def extract_and_load_func(**dictionary):
    print("Connecting to SOURCE database...")
    #EXTRACT
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

    df = pd.DataFrame(rows)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

    #LOAD
    print("Connecting to REPORTING database...")
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        print("Successfully connected to REPORTING database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to reporting DB: {e}")
        return
    
    table_name = "ethisx_reporting.bank"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL UNIQUE,
        bank_name VARCHAR(255),
        ac_name VARCHAR(255),
        ac_no VARCHAR(100),
        branch VARCHAR(255),
        swift_code VARCHAR(50),
        iban VARCHAR(100),
        home_address TEXT,
        UNIQUE KEY unique_user (id, user_id)
    );
    """
    dest_cur.execute(create_table)

    #BATCH INSERT
    batch_size = 500
    columns = [col for col in df.columns if col != "id"]   #skip id since it's AUTO_INCREMENT
    col_names = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    insertion = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"

    values = []
    for _, row in df.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()

        #Replace NaN with None for MySQL
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        values.append([dict_row[col] for col in columns])

        #Insert in batches
        if len(values) >= batch_size:
            dest_cur.executemany(insertion, values)
            dest_conn.commit()
            values.clear()

    #Insert remaining rows
    if values:
        dest_cur.executemany(insertion, values)
        dest_conn.commit()

    dest_cur.close()
    dest_conn.close()
    print(f"Inserted {len(df)} rows into {table_name}.")
    print("Reporting DB connection closed.")

# ------------------ DAG ------------------ #
with DAG(
    dag_id="bank_airflow",
    start_date=days_ago(1),                 #starts yesterday, so scheduler will pick it up
    schedule_interval="0 */6 * * *",        #every 6 hours
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
        execution_timeout=timedelta(minutes=30),  #increase timeout
    )