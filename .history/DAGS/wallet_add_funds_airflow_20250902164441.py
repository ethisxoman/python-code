import math
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """
    -- WALLET ADD FUNDS DATA
    SELECT 
        fu.user_id, 
        a.amount AS add_funds_amount, 
        a.currency AS add_funds_currency,
        a.base_currency AS add_funds_base_currency_USD, 
        a.converted_amount AS add_funds_converted_amount_USD,  
        a.updated_amount AS add_funds_updated_amount, 
        a.date AS add_funds_date, 

        CASE 
            WHEN a.payment_type = 1 THEN 'Bank' 
            WHEN a.payment_type = 2 THEN 'Credit/Debit' 
            ELSE NULL 
        END AS add_funds_payment_type, 

        a.transaction_id AS add_funds_transaction_id, 
        a.is_approved AS add_funds_status, 
        a.created_at AS add_funds_created_at, 
        a.updated_at AS add_funds_updated_at 

    FROM ethisx_accounts.users u 
    INNER JOIN ethisx_wallet_live.fund_users fu 
        ON u.id = fu.user_id
    LEFT JOIN ethisx_wallet_live.add_funds a	
        ON fu.fund_id = a.id
    ORDER BY u.id;
"""

# ------------------ EXTRACT + LOAD ------------------ #
def extract_and_load_func(**dictionary):
    print("Connecting to SOURCE database...")
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
        df = pd.DataFrame(rows, columns=columns)
        print(f"Query executed. DataFrame created with {len(df)} rows and {len(df.columns)} columns.")
    except Exception as e:
        print(f"[ERROR] Failed executing query: {e}")
        src_conn.close()
        return

    src_conn.close()
    print("Source DB connection closed.")

    if df.empty:
        print("No rows found in source database.")
        return

    # ------------------ LOAD ------------------ #
    print("Connecting to REPORTING database...")
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        print("Successfully connected to REPORTING database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to reporting DB: {e}")
        return
    
    table_name = "ethisx_reporting.wallet_add_funds"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        add_funds_amount DECIMAL(18,2),
        add_funds_currency VARCHAR(10),
        add_funds_base_currency_USD VARCHAR(10),
        add_funds_converted_amount_USD DECIMAL(15,2),
        add_funds_updated_amount DECIMAL(15,2),
        add_funds_date DATE,
        add_funds_payment_type VARCHAR(50),
        add_funds_transaction_id VARCHAR(255) NOT NULL,
        add_funds_status VARCHAR(255),            
        add_funds_created_at TIMESTAMP,
        add_funds_updated_at TIMESTAMP,
        UNIQUE KEY unique_user (add_funds_transaction_id)
    );
    """
    dest_cur.execute(create_table)

    # Insert row by row (no executemany)
    for _, row in df.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()
        # Replace NaN/NaT with None for MySQL
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
    print(f"Inserted {len(df)} rows into {table_name}.")
    print("Reporting DB connection closed.")

# ------------------ DAG ------------------ #
with DAG(
    dag_id="wallet_add_funds_airflow",
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",   # every 6 hours
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
    )
