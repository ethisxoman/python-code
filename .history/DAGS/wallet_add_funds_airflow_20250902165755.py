import math
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

# Source query (all wallet add funds records)
query = """
    SELECT DISTINCT
        fu.user_id, 
        a.amount AS add_funds_amount, 
        a.currency AS add_funds_currency,
        a.base_currency AS add_funds_base_currency_USD, 
        a.converted_amount AS add_funds_converted_amount_USD,  
        a.updated_amount AS add_funds_updated_amount, 
        a.date AS add_funds_date, 

        CASE 
            WHEN a.payment_type = 1 THEN "Bank" 
            WHEN a.payment_type = 2 THEN "Credit/Debit" 
            ELSE NULL 
        END AS add_funds_payment_type, 

        a.transaction_id AS add_funds_transaction_id, 
        a.is_approved AS add_funds_status, 
        a.created_at AS add_funds_created_at, 
        a.updated_at AS add_funds_updated_at 

    FROM ethisx_accounts.users u 
    INNER JOIN ethisx_wallet_live.fund_users fu 
        ON u.id = fu.user_id
    LEFT JOIN ethisx_wallet_live.add_funds AS a	
        ON fu.fund_id = a.id
    WHERE a.id IS NOT NULL
    ORDER BY u.id, a.id
"""

def extract_and_load_upsert(**dictionary):
    """UPSERT approach - handles inserts and updates to existing transactions"""
    print("Starting UPSERT wallet pipeline...")
    
    # ----------------- SOURCE ----------------- #
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

    # ----------------- DESTINATION ----------------- #
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
        record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        
        -- One record per user per transaction
        UNIQUE KEY unique_user_transaction (user_id, add_funds_transaction_id),
        INDEX idx_transaction_id (add_funds_transaction_id),
        INDEX idx_user_id (user_id),
        INDEX idx_updated_at (add_funds_updated_at)
    );
    """
    dest_cur.execute(create_table)

    # Count before
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_before = dest_cur.fetchone()[0]
    print(f"Records in table before insertion: {count_before}")

    # Deduplicate within DataFrame
    df_deduplicated = df.drop_duplicates(subset=['user_id', 'add_funds_transaction_id'], keep='first')
    print(f"After deduplication: {len(df_deduplicated)} rows (removed {len(df) - len(df_deduplicated)} duplicates)")

    # UPSERT query
    columns = [col for col in df_deduplicated.columns]
    col_names = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [col for col in columns if col not in ['user_id', 'add_funds_transaction_id']]
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
    
    upsert_query = f"""
    INSERT INTO {table_name} ({col_names}) 
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE 
    {update_clause}
    """

    # Batch UPSERT
    batch_size = 500
    values = []
    processed_count = 0

    for _, row in df_deduplicated.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()
        # Replace NaN with None for MySQL
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        values.append([dict_row[col] for col in columns])

        if len(values) >= batch_size:
            try:
                dest_cur.executemany(upsert_query, values)
                dest_conn.commit()
                processed_count += len(values)
                print(f"Processed batch of {len(values)} rows. Total processed: {processed_count}")
                values.clear()
            except Exception as e:
                print(f"[ERROR] Failed to upsert batch: {e}")
                dest_conn.rollback()
                values.clear()

    if values:
        try:
            dest_cur.executemany(upsert_query, values)
            dest_conn.commit()
            processed_count += len(values)
            print(f"Processed final batch of {len(values)} rows.")
        except Exception as e:
            print(f"[ERROR] Failed to upsert final batch: {e}")
            dest_conn.rollback()

    # Count after
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_after = dest_cur.fetchone()[0]

    dest_cur.close()
    dest_conn.close()
    
    print(f"Processing complete:")
    print(f"- Records before: {count_before}")
    print(f"- Records after: {count_after}")
    print(f"- New or updated records: {count_after - count_before}")
    print(f"- Total rows processed: {processed_count}")
    print("Reporting DB connection closed.")

# ----------------- DAG ----------------- #
with DAG(
    dag_id="wallet_add_funds_no_duplicates",
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",   
    catchup=False,
    max_active_runs=1,  # Prevent parallel runs
    tags=['wallet', 'etl', 'no-duplicates']
) as dag:

    task = PythonOperator(
        task_id="extract_and_load_upsert",
        python_callable=extract_and_load_upsert,
        execution_timeout=timedelta(minutes=30),
    )
