# import math
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from connection import get_airflow_connection, source_db, reporting_db

# print("Starting pipeline script...")

# query = """
#     #---------------WALLET ADD FUNDS DATA----------------#
#     SELECT 

#     fu.user_id, 
#     a.amount AS add_funds_amount, a.currency AS add_funds_currency,

#     a.base_currency AS add_funds_base_currency_USD, a.converted_amount AS add_funds_converted_amount_USD,  
#     a.updated_amount AS add_funds_updated_amount, 
#     a.date AS add_funds_date, 

#     case when a.payment_type = 1 then "Bank" 
#     when a.payment_type = 2 then "Credit/Debit" 
#     else null end AS add_funds_payment_type, 

#     a.transaction_id AS add_funds_transaction_id, a.is_approved AS add_funds_status, 
#     a.created_at AS add_funds_created_at, a.updated_at AS add_funds_updated_at 

#     from ethisx_accounts.users u 
#     inner join ethisx_wallet_live.fund_users fu 
#     on u.id = fu.user_id

#     LEFT JOIN ethisx_wallet_live.add_funds AS a	
#     ON fu.fund_id = a.id

#     order by u.id

# """

# #extract and load
# def extract_and_load_func(**dictionary):
#     print("Connecting to SOURCE database...")
#     #extract
#     try:
#         src_conn = get_airflow_connection(source_db)
#         src_cur = src_conn.cursor()
#         print("Successfully connected to SOURCE database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to source DB: {e}")
#         return

#     print("Executing source query...")
#     try:
#         src_cur.execute(query)
#         rows = src_cur.fetchall()

#         # ✅ FIX: add this right here
#         columns = [desc[0] for desc in src_cur.description]
#         df = pd.DataFrame(rows, columns=columns)
#         print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns: {df.columns.tolist()}")

#     except Exception as e:
#         print(f"[ERROR] Failed executing query: {e}")
#         src_conn.close()
#         return

#     src_conn.close()
#     print("Source DB connection closed.")


#     if not rows:
#         print("No rows found in source database.")
#         return

#     # df = pd.DataFrame(rows)
#     print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

#     #load
#     print("Connecting to REPORTING database...")
#     try:
#         dest_conn = get_airflow_connection(reporting_db)
#         dest_cur = dest_conn.cursor()
#         print("Successfully connected to REPORTING database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to reporting DB: {e}")
#         return
    
#     table_name = "ethisx_reporting.wallet_add_funds"
#     print(f"Ensuring table exists: {table_name}")

#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
    
    
#     /* Wallet: Add Funds */
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     user_id INT NOT NULL,

#     add_funds_amount DECIMAL(18,2),
#     add_funds_currency VARCHAR(10),

#     add_funds_base_currency_USD VARCHAR(10),
#     add_funds_converted_amount_USD DECIMAL(15,2),

#     add_funds_updated_amount DECIMAL(15,2),
#     add_funds_date DATE,

#     add_funds_payment_type BIGINT,

#     add_funds_transaction_id VARCHAR(255) NOT NULL,
#     add_funds_status VARCHAR(255),            
#     add_funds_created_at TIMESTAMP,
#     add_funds_updated_at TIMESTAMP,
#     UNIQUE KEY unique_user (add_funds_transaction_id)
# );


#     """
#     dest_cur.execute(create_table)

#     for _, row in df.reset_index(drop=True).iterrows():
#         dict_row = row.to_dict()

#         #replace NaN with None for MySQL
#         for k, v in dict_row.items():
#             if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
#                 dict_row[k] = None

#         columns = list(dict_row.keys())
#         col_names = ", ".join([f"`{col}`" for col in columns])
#         placeholders = ", ".join(["%s"] * len(columns))

#         insertion = f"""
#         INSERT IGNORE INTO {table_name} ({col_names})
#         VALUES ({placeholders})
#         """  
  
#         dest_cur.execute(insertion, list(dict_row.values()))

#     dest_conn.commit()
#     dest_cur.close()
#     dest_conn.close()
#     print(f"Inserted {len(df)} new rows into {table_name}.")
#     print("Reporting DB connection closed.")

# #DAG
# with DAG(
#     dag_id="wallet_add_funds_airflow",
#     start_date=days_ago(1),      #starts yesterday, so scheduler will pick it up
#     schedule_interval="0 */6 * * *",   
#     catchup=False,
# ) as dag:

#     task = PythonOperator(
#         task_id="extract_and_load",
#         python_callable=extract_and_load_func,
#     )
#############################################################################################################
import math
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting incremental pipeline script...")

def get_last_processed_timestamp():
    """Get the last processed timestamp from the reporting database"""
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        
        check_query = """
        SELECT MAX(add_funds_updated_at) as last_timestamp
        FROM ethisx_reporting.wallet_add_funds
        WHERE add_funds_updated_at IS NOT NULL
        """
        
        dest_cur.execute(check_query)
        result = dest_cur.fetchone()
        dest_conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return '2024-01-01 00:00:00'  # Default start date if no data
            
    except Exception as e:
        print(f"[INFO] Could not get last timestamp (table might not exist): {e}")
        return '2024-01-01 00:00:00'

def extract_and_load_incremental(**dictionary):
    """Incremental approach - only processes new/updated records"""
    print("Getting last processed timestamp...")
    last_timestamp = get_last_processed_timestamp()
    print(f"Last processed timestamp: {last_timestamp}")
    
    # Create incremental query
    incremental_query = f"""
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
      AND (a.created_at >= '{last_timestamp}' OR a.updated_at >= '{last_timestamp}')
    ORDER BY u.id, a.id
    """
    
    print("Connecting to SOURCE database...")
    try:
        src_conn = get_airflow_connection(source_db)
        src_cur = src_conn.cursor()
        print("Successfully connected to SOURCE database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to source DB: {e}")
        return

    print("Executing incremental source query...")
    try:
        src_cur.execute(incremental_query)
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
        print("No new or updated rows found in source database.")
        return

    df = pd.DataFrame(rows, columns=columns)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

    # Remove duplicates within the DataFrame
    df_deduplicated = df.drop_duplicates(subset=['user_id', 'add_funds_transaction_id'], keep='first')
    print(f"After deduplication: {len(df_deduplicated)} rows (removed {len(df) - len(df_deduplicated)} duplicates)")

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
        UNIQUE KEY unique_user_transaction (user_id, add_funds_transaction_id),
        INDEX idx_updated_at (add_funds_updated_at)
    );
    """
    dest_cur.execute(create_table)

    # Get count before
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_before = dest_cur.fetchone()[0]
    print(f"Records before: {count_before}")

    # Get existing business keys to check for duplicates
    existing_query = """
    SELECT CONCAT(user_id, '|', add_funds_transaction_id) as business_key
    FROM ethisx_reporting.wallet_add_funds
    """
    dest_cur.execute(existing_query)
    existing_records = dest_cur.fetchall()
    existing_keys = {record[0] for record in existing_records}
    print(f"Found {len(existing_keys)} existing transaction records.")

    # Filter out existing records
    new_rows = []
    duplicate_count = 0
    
    for _, row in df_deduplicated.reset_index(drop=True).iterrows():
        row_dict = row.to_dict()
        
        # Create business key
        business_key = f"{row_dict.get('user_id', '')}|{row_dict.get('add_funds_transaction_id', '')}"
        
        if business_key not in existing_keys:
            # Replace NaN with None for MySQL
            for k, v in row_dict.items():
                if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                    row_dict[k] = None
            
            new_rows.append(row_dict)
            existing_keys.add(business_key)
        else:
            duplicate_count += 1

    print(f"Found {len(new_rows)} new records to insert.")
    print(f"Skipped {duplicate_count} duplicate records.")

    if not new_rows:
        print("No new data to insert. Exiting.")
        dest_conn.close()
        return

    # Batch insert only new records
    batch_size = 500
    columns = [col for col in df_deduplicated.columns]
    col_names = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    
    insertion = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"

    values = []
    inserted_count = 0
    
    for row_dict in new_rows:
        values.append([row_dict.get(col) for col in columns])

        if len(values) >= batch_size:
            try:
                dest_cur.executemany(insertion, values)
                dest_conn.commit()
                inserted_count += len(values)
                print(f"Inserted batch of {len(values)} rows. Total inserted: {inserted_count}")
                values.clear()
            except Exception as e:
                print(f"[ERROR] Failed to insert batch: {e}")
                dest_conn.rollback()
                values.clear()

    if values:
        try:
            dest_cur.executemany(insertion, values)
            dest_conn.commit()
            inserted_count += len(values)
            print(f"Inserted final batch of {len(values)} rows.")
        except Exception as e:
            print(f"[ERROR] Failed to insert final batch: {e}")
            dest_conn.rollback()

    # Get final count
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_after = dest_cur.fetchone()[0]

    dest_cur.close()
    dest_conn.close()
    
    print(f"Incremental Processing complete:")
    print(f"- Records before: {count_before}")
    print(f"- Records after: {count_after}")
    print(f"- New records added: {count_after - count_before}")
    print("Reporting DB connection closed.")

# DAG
with DAG(
    dag_id="wallet_add_funds_incremental",
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",   
    catchup=False,
    max_active_runs=1,
    tags=['wallet', 'incremental']
) as dag:

    task = PythonOperator(
        task_id="extract_and_load_incremental",
        python_callable=extract_and_load_incremental,
        execution_timeout=timedelta(minutes=30),
    )