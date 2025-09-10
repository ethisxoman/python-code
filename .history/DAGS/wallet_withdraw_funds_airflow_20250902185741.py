import math
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """
    #---------------WALLET WITHDRAW FUNDS----------------#
       

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
    user_id BIGINT NOT NULL,
    withdraw_funds_amount VARCHAR(255),
    withdraw_funds_currency VARCHAR(3),
    withdraw_funds_base_currency VARCHAR(3),
    withdraw_funds_exchange_rate DECIMAL(16,8),
    withdraw_funds_converted_amount DECIMAL(16,2),
    withdraw_funds_remaining_balance VARCHAR(255),
    withdraw_funds_bank_id VARCHAR(255),
    withdraw_funds_payment_type BIGINT(20),
    withdraw_funds_status BIGINT,
    withdraw_funds_created_at DATETIME,
    withdraw_funds_updated_at DATETIME,
    
    bank_name VARCHAR(255),
    account_name VARCHAR(255),
    account_num VARCHAR(255),
    country_id BIGINT,
    branch VARCHAR(255),
    iban VARCHAR(255),
    home_address TEXT,
    swift_code VARCHAR(255),

    UNIQUE KEY uniq_user_withdraw (user_id, withdraw_funds_created_at)
);
    """
    dest_cur.execute(create_table)

    for _, row in df.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()

        #replace NaN with None for MySQL
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        columns = list(dict_row.keys())
        col_names = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        insertion = f"""
        INSERT IGNORE INTO {table_name} ({col_names})
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