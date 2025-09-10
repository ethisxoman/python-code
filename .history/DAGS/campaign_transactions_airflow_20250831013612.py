from datetime import datetime
import math
import mysql.connector
import pandas as pd
# from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """SELECT 
        ct.id,
        ct.payment_request_id,
        ct.issuer_disbursed_request_id, 
        ct.payment_request_id,
        ct.investor_disbursed_request_id,
        ct.pre_campaign_fee_request_id, 
        ct.campaign_id AS campaign_transactions_id,
        ct.investor_id,
        ct.issuer_id,
        ct.transaction_no, ct.transaction_type,
        ct.transaction_status,
        ct.payment_method,
        ct.amount, ct.charge, ct.currency, ct.rate,
        ct.base_amount, ct.base_currency,
        ct.investment_status, ct.status,
        ct.remarks, ct.transaction_date,
        ct.deal_id, ct.deleted_at, ct.created_at, 
        ct.updated_at, ct.affiliate_code
        
FROM ethisxadmin.campaign_transactions AS ct
WHERE ct.id IS NOT NULL;
"""

#extract and load
def extract_and_load_func(**dictionary):
    print("Connecting to SOURCE database...")
    #extract
    try:
        src_conn = mysql.connector.connect(**source_db)
        src_cur = src_conn.cursor(dictionary=True)
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
        dest_conn = mysql.connector.connect(**reporting_db)
        dest_cur = dest_conn.cursor()
        print("Successfully connected to REPORTING database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to reporting DB: {e}")
        return

    table_name = "ethisx_reporting.campaign_transactions"
    print(f"Ensuring table exists: {table_name}")
        
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGINT PRIMARY KEY,
        payment_request_id BIGINT,
        issuer_disbursed_request_id BIGINT,
        investor_disbursed_request_id BIGINT,
        pre_campaign_fee_request_id BIGINT,
        campaign_transactions_id BIGINT,
        investor_id BIGINT,
        issuer_id BIGINT,
        transaction_no VARCHAR(100),
        transaction_type VARCHAR(100),
        transaction_status VARCHAR(100),
        payment_method VARCHAR(100),
        amount DECIMAL(18,2),
        charge DECIMAL(18,2),
        currency VARCHAR(10),
        rate DECIMAL(18,6),
        base_amount DECIMAL(18,2),
        base_currency VARCHAR(10),
        investment_status VARCHAR(100),
        status VARCHAR(50),
        remarks TEXT,
        transaction_date DATETIME,
        deal_id BIGINT,
        deleted_at DATETIME,
        created_at DATETIME,
        updated_at DATETIME,
        affiliate_code VARCHAR(100)
        
        -- UNIQUE KEY uniq_trans_id (id)
);

    """
    dest_cur.execute(create_table)

    for _, row in df.iterrows():
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
    dag_id="campaign_report_pipeline",
    start_date=days_ago(1),      #starts yesterday, so scheduler will pick it up
    schedule_interval="0 */6 * * *",   
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
    )