from datetime import datetime
import math
import mysql.connector
import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
from connection import source_db, reporting_db

print("Starting pipeline script...")

query = """SELECT 
        rt.id,
        rt.investor_id AS referred_user_id,
        rt.campaign_id,
        rt.commission_amount AS commission_amount,
        rt.commission_percentage AS commission_percentage,
        rt.ref_id AS referral_user_id,
        rt.status,
        rt.created_at,
        rt.updated_at
FROM ethisxadmin.referral_transactions AS rt
WHERE rt.id IS NOT NULL;
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

    table_name = "ethisx_reporting.referrals"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (

        -- referral_transactions columns
        id BIGINT PRIMARY KEY,
        referred_user_id BIGINT,
        campaign_id BIGINT,
        commission_amount DECIMAL(18,2),
        commission_percentage DECIMAL(10,2),
        referral_user_id INT,
        status VARCHAR(50),
        created_at DATETIME,
        updated_at DATETIME,

        UNIQUE KEY uniq_referral_rt (id)
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

# #DAG
# with DAG(
#     dag_id="referrals_report_pipeline",
#     start_date=datetime(2025, 8, 20),
#     schedule_interval="@daily",
#     catchup=False
# ) as dag:

#     task = PythonOperator(
#         task_id="extract_and_load",
#         python_callable=extract_and_load_func,
#         provide_context=True
#     )

if __name__ == "__main__":
    print("Running in standalone mode...")
    extract_and_load_func()