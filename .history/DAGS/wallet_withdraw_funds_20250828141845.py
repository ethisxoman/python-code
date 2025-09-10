# from datetime import datetime
# import math
# import mysql.connector
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from connection import source_db, reporting_db

# print("Starting pipeline script...")

# query = """
#     #---------------WALLET WITHDRAW FUNDS----------------#
#     SELECT 
      
# w.user_id, w.amount AS withdraw_funds_amount, w.currency AS withdraw_funds_currency, 
# w.base_currency AS withdraw_funds_base_currency, w.exchange_rate AS withdraw_funds_exchange_rate, 
# w.converted_amount AS withdraw_funds_converted_amount, w.remaining_balance AS withdraw_funds_remaining_balance, 
# w.bank AS withdraw_funds_bank_id, 
#     case when 	 w.payment_type = 1 then "Bank" 
#     when w.payment_type = 2 then "Credit/Debit" 
#     else null end AS withdraw_funds_payment_type, 

# w.is_processed AS withdraw_funds_status, w.created_at AS withdraw_funds_created_at, 
# w.updated_at AS withdraw_funds_updated_at, 

# ub.bank_name, ub.ac_name AS account_name, ub.ac_no AS account_num, ub.country_id, 
# ub.branch, ub.iban, ub.home_address, ub.swift_code

# FROM ethisx_accounts.users u 

# INNER JOIN ethisx_wallet_live.withdraw_funds AS w 
# 	ON u.id = w.user_id

# LEFT JOIN ethisxadmin.user_bank_infos AS ub
# 	ON u.id = ub.user_id 
# order by u.id

# """

# #extract and load
# def extract_and_load_func(**dictionary):
#     print("Connecting to SOURCE database...")
#     #extract
#     try:
#         src_conn = mysql.connector.connect(**source_db)
#         src_cur = src_conn.cursor(dictionary=True)
#         print("Successfully connected to SOURCE database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to source DB: {e}")
#         return

#     print("Executing source query...")
#     try:
#         src_cur.execute(query)
#         rows = src_cur.fetchall()
#         print(f"Query executed. Rows fetched: {len(rows)}")
#     except Exception as e:
#         print(f"[ERROR] Failed executing query: {e}")
#         src_conn.close()
#         return

#     src_conn.close()
#     print("Source DB connection closed.")

#     if not rows:
#         print("No rows found in source database.")
#         return

#     df = pd.DataFrame(rows)
#     print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")
    
#     #Add debug prints here
#     print(f"Sample row from DataFrame: {df.head(1).to_dict(orient='records')}")
#     print(f"Inserting {len(df)} rows into {table_name}...")

#     #load
#     print("Connecting to REPORTING database...")
#     try:
#         dest_conn = mysql.connector.connect(**reporting_db)
#         dest_cur = dest_conn.cursor()
#         print("Successfully connected to REPORTING database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to reporting DB: {e}")
#         return

#     table_name = "ethisx_reporting.wallet_withdraw_funds"
#     print(f"Ensuring table exists: {table_name}")

#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
    
#     /* Wallet: Add Funds */
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     -- Wallet: Withdraw Funds
#         user_id BIGINT,
#         withdraw_funds_amount VARCHAR(255),
#         withdraw_funds_currency VARCHAR(3),
#         withdraw_funds_base_currency VARCHAR(3),
#         withdraw_funds_exchange_rate DECIMAL(16,8),
#         withdraw_funds_converted_amount DECIMAL(16,2),
#         withdraw_funds_remaining_balance VARCHAR(255),
#         withdraw_funds_bank_id VARCHAR(255),
#         withdraw_funds_payment_type BIGINT(20),
#         withdraw_funds_status BIGINT,
#         withdraw_funds_created_at DATETIME,
#         withdraw_funds_updated_at DATETIME,
        
#         -- Bank Info
#         bank_name VARCHAR(255),
#         account_name VARCHAR(255),
#         account_num VARCHAR(255),
#         country_id BIGINT,
#         branch VARCHAR(255),
#         iban VARCHAR(255),
#         home_address TEXT,
#         swift_code VARCHAR(255)
# );


#     """
#     dest_cur.execute(create_table)

#     # for _, row in df.iterrows():
#     #     dict_row = row.to_dict()

#     #     #replace NaN with None for MySQL
#     #     for k, v in dict_row.items():
#     #         if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
#     #             dict_row[k] = None

#     #     columns = list(dict_row.keys())
#     #     col_names = ", ".join([f"`{col}`" for col in columns])
#     #     placeholders = ", ".join(["%s"] * len(columns))

#     #     insertion = f"""
#     #     INSERT IGNORE INTO {table_name} ({col_names})
#     #     VALUES ({placeholders})
#     #     """  
    
#     for _, row in df.iterrows():
#         dict_row = row.to_dict()
#     for k, v in dict_row.items():
#         if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
#             dict_row[k] = None

#     columns = list(dict_row.keys())
#     col_names = ", ".join([f"`{col}`" for col in columns])
#     placeholders = ", ".join(["%s"] * len(columns))

#     insertion = f"""
#     INSERT INTO {table_name} ({col_names})
#     VALUES ({placeholders})
#     """  

#     try:
#         dest_cur.execute(insertion, list(dict_row.values()))
#     except Exception as e:
#         print(f"[ERROR] Insert failed for row {dict_row}: {e}")

  
#         dest_cur.execute(insertion, list(dict_row.values()))

#     dest_conn.commit()
#     dest_cur.close()
#     dest_conn.close()
#     print(f"Inserted {len(df)} new rows into {table_name}.")
#     print("Reporting DB connection closed.")

# #DAG
# with DAG(
#     dag_id="withdraw_funds_pipeline",
#     start_date=datetime(2025, 8, 20),
#     schedule_interval="* * * * *",
#     catchup=False
# ) as dag:

#     task = PythonOperator(
#         task_id="extract_and_load",
#         python_callable=extract_and_load_func,
#         provide_context=True
#     )

# if __name__ == "__main__":
#     print("Running in standalone mode...")
#     extract_and_load_func()
# #-----------------------------------------------------------
from datetime import datetime
import math
import pandas as pd
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from connection import source_db, reporting_db

print("Starting pipeline script...")

query = """
    #---------------WALLET WITHDRAW FUNDS----------------#
    SELECT 
      
w.user_id, w.amount AS withdraw_funds_amount, w.currency AS withdraw_funds_currency, 
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
        src_conn = pymysql.connect(**source_db, cursorclass=pymysql.cursors.DictCursor)
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
    print(f"Sample row from DataFrame: {df.head(1).to_dict(orient='records')}")

    #load
    print("Connecting to REPORTING database...")
    try:
        dest_conn = pymysql.connect(**reporting_db)
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
    -- Wallet: Withdraw Funds
        user_id BIGINT,
        withdraw_funds_amount VARCHAR(255),
        withdraw_funds_currency VARCHAR(3),
        withdraw_funds_base_currency VARCHAR(3),
        withdraw_funds_exchange_rate DECIMAL(16,8),
        withdraw_funds_converted_amount DECIMAL(16,2),
        withdraw_funds_remaining_balance VARCHAR(255),
        withdraw_funds_bank_id VARCHAR(255),
        withdraw_funds_payment_type BIGINT,
        withdraw_funds_status BIGINT,
        withdraw_funds_created_at DATETIME,
        withdraw_funds_updated_at DATETIME,
        
        -- Bank Info
        bank_name VARCHAR(255),
        account_name VARCHAR(255),
        account_num VARCHAR(255),
        country_id BIGINT,
        branch VARCHAR(255),
        iban VARCHAR(255),
        home_address TEXT,
        swift_code VARCHAR(255)
);
    """
    dest_cur.execute(create_table)

    print(f"Inserting {len(df)} rows into {table_name}...")

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
    dag_id="withdraw_funds_pipeline",
    start_date=datetime(2025, 8, 27),
    schedule_interval="* * * * *",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
        provide_context=True
    )

if __name__ == "__main__":
    print("Running in standalone mode...")
    extract_and_load_func()

#-----------------------
# from datetime import datetime
# import math
# import mysql.connector
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from connection import source_db, reporting_db

# print("Starting pipeline script...")

# query = """
#     #---------------WALLET WITHDRAW FUNDS----------------#
#     SELECT 
      
# w.user_id, w.amount AS withdraw_funds_amount, w.currency AS withdraw_funds_currency, 
# w.base_currency AS withdraw_funds_base_currency, w.exchange_rate AS withdraw_funds_exchange_rate, 
# w.converted_amount AS withdraw_funds_converted_amount, w.remaining_balance AS withdraw_funds_remaining_balance, 
# w.bank AS withdraw_funds_bank_id, 
#     case when 	 w.payment_type = 1 then "Bank" 
#     when w.payment_type = 2 then "Credit/Debit" 
#     else null end AS withdraw_funds_payment_type, 

# w.is_processed AS withdraw_funds_status, w.created_at AS withdraw_funds_created_at, 
# w.updated_at AS withdraw_funds_updated_at, 

# ub.bank_name, ub.ac_name AS account_name, ub.ac_no AS account_num, ub.country_id, 
# ub.branch, ub.iban, ub.home_address, ub.swift_code

# FROM ethisx_accounts.users u 

# INNER JOIN ethisx_wallet_live.withdraw_funds AS w 
# 	ON u.id = w.user_id

# LEFT JOIN ethisxadmin.user_bank_infos AS ub
# 	ON u.id = ub.user_id 
# order by u.id

# """

# #extract and load
# def extract_and_load_func(**dictionary):
#     print("Connecting to SOURCE database...")
#     #extract
#     try:
#         src_conn = mysql.connector.connect(**source_db)
#         src_cur = src_conn.cursor(dictionary=True)
#         print("Successfully connected to SOURCE database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to source DB: {e}")
#         return

#     print("Executing source query...")
#     try:
#         src_cur.execute(query)
#         rows = src_cur.fetchall()
#         print(f"Query executed. Rows fetched: {len(rows)}")
#     except Exception as e:
#         print(f"[ERROR] Failed executing query: {e}")
#         src_conn.close()
#         return

#     src_conn.close()
#     print("Source DB connection closed.")

#     if not rows:
#         print("No rows found in source database.")
#         return

#     df = pd.DataFrame(rows)
#     print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

#     #load
#     print("Connecting to REPORTING database...")
#     try:
#         dest_conn = mysql.connector.connect(**reporting_db)
#         dest_cur = dest_conn.cursor()
#         print("Successfully connected to REPORTING database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to reporting DB: {e}")
#         return

#     table_name = "ethisx_reporting.wallet_withdraw_funds"
#     print(f"Ensuring table exists: {table_name}")

#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
    
#     /* Wallet: Add Funds */
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     -- Wallet: Withdraw Funds
#         user_id BIGINT,
#         withdraw_funds_amount VARCHAR(255),
#         withdraw_funds_currency VARCHAR(3),
#         withdraw_funds_base_currency VARCHAR(3),
#         withdraw_funds_exchange_rate DECIMAL(16,8),
#         withdraw_funds_converted_amount DECIMAL(16,2),
#         withdraw_funds_remaining_balance VARCHAR(255),
#         withdraw_funds_bank_id VARCHAR(255),
#         withdraw_funds_payment_type BIGINT(20),
#         withdraw_funds_status BIGINT,
#         withdraw_funds_created_at DATETIME,
#         withdraw_funds_updated_at DATETIME,
        
#         -- Bank Info
#         bank_name VARCHAR(255),
#         account_name VARCHAR(255),
#         account_num VARCHAR(255),
#         country_id BIGINT,
#         branch VARCHAR(255),
#         iban VARCHAR(255),
#         home_address TEXT,
#         swift_code VARCHAR(255)
# );


#     """
#     dest_cur.execute(create_table)

#     for _, row in df.iterrows():
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
#     dag_id="withdraw_funds_pipeline",
#     start_date=datetime(2025, 8, 28),
#     schedule="* * * * *",
#     catchup=False
# ) as dag:

#     task = PythonOperator(
#         task_id="extract_and_load",
#         python_callable=extract_and_load_func,

#     )

# if __name__ == "__main__":
#     print("Running in standalone mode...")
#     extract_and_load_func()