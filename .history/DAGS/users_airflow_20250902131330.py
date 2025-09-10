# import math
# import pandas as pd
# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from connection import get_airflow_connection, source_db, reporting_db

# print("Starting incremental pipeline script...")

# def get_last_created_date():
#     """Get the latest created_at date from the reporting table"""
#     try:
#         dest_conn = get_airflow_connection(reporting_db)
#         dest_cur = dest_conn.cursor()
        
#         # Get the maximum created_at date from reporting table
#         dest_cur.execute("""
#             SELECT MAX(user_created_at) 
#             FROM ethisx_reporting.users
#         """)
        
#         result = dest_cur.fetchone()
#         dest_conn.close()
        
#         if result and result[0]:
#             return result[0].strftime('%Y-%m-%d %H:%M:%S')
#         else:
#             # If no records exist, start from a default date
#             return '2025-02-01 00:00:00'
            
#     except Exception as e:
#         print(f"[WARNING] Could not get last created date: {e}")
#         # Fallback to default date
#         return '2025-02-01 00:00:00'

# def extract_and_load_func(**dictionary):
#     print("Getting last processed date...")
#     last_created_date = get_last_created_date()
#     print(f"Last processed date: {last_created_date}")
    
#     # Modified query to only get records newer than the last processed date
#     query = f"""SELECT DISTINCT
#         u.id AS user_id, u.email, 
#         CONCAT(u.first_name, ' ', u.last_name) AS full_name,
#         u.role_id,  u.email_verified_at, u.investor_category_id, u.phone_code, u.phone, 
#         u.is_approved, u.login_with, u.is_admin,
#         u.created_at AS user_created_at, u.updated_at AS user_updated_at, u.gender, u.dob, u.namescan_id, 
#         u.namescan_match_rate, u.kyc_general_match_status, u.platform_ref, u.maala_result,

#         cntrs.name AS country,
#         c.type AS user_type,
#         a.aml_record,

#         ke.id AS kyc_id, ke.kyc_info_id, ke.investor_category_id AS kyc_investor_category_id, ke.annual_salary, ke.net_worth, 
#         ke.status AS kyc_status,

#         ic.title AS investor_category_title,
#         ic.type AS investor_category_type,

#         #---------------ADMIN----------------#
#         ub.bank_name, ub.ac_name, ub.ac_no, ub.branch, ub.swift_code, ub.iban, ub.home_address,

#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.CivilNumber')) AS Malaa_Civil_Number,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_1_En')) AS Malaa_Name1,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_2_En')) AS Malaa_Name2,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_3_En')) AS Malaa_Name3,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_6_En')) AS Malaa_LastName,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Gender_Desc_En')) AS Malaa_Gender,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Birth_DateOfBirth')) AS Malaa_Date_Of_Birth,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Birth_Country_Desc_En')) AS Malaa_Birth_Country,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Address_Wilayat_Desc_En')) AS Malaa_Address,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Current_MobileNumber')) AS Malaa_Mobile,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Occupation_Desc_En')) AS Malaa_Occupation,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_PassportNumber')) AS Malaa_Passport_Number,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_DateOfIssue')) AS Malaa_Passport_Issue_Date,
#         JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_DateOfExpiry')) AS Malaa_Passport_Expiry_Date

#     FROM ethisx_accounts.users AS u
#     LEFT JOIN ethisx_accounts.countries AS cntrs 
#         ON u.country_id = cntrs.id
#     LEFT JOIN ethisx_accounts.cities AS ct
#         ON u.country_id = ct.id
#     LEFT JOIN ethisx_accounts.user_types AS b 
#         ON u.id = b.user_id
#     LEFT JOIN ethisx_accounts.types AS c 
#         ON b.type_id = c.id
#     LEFT JOIN ethisx_accounts.kyc_infos AS k
#         ON u.id = k.namescan_id
#     LEFT JOIN ethisx_accounts.kyc_ex_platforms AS ke
#         ON k.id = ke.kyc_info_id
#     LEFT JOIN ethisx_accounts.states AS s
#         ON u.id = s.id
#     LEFT JOIN ethisx_accounts.aml_stores AS a
#         ON u.id = a.id
#     LEFT JOIN ethisx_accounts.investor_categories AS ic
#         ON u.investor_category_id = ic.id
#     LEFT JOIN ethisx_accounts.user_types AS utp
#         ON u.id = utp.user_id
#     LEFT JOIN ethisxadmin.user_bank_infos AS ub
#         ON u.id = ub.user_id    
#     WHERE u.id IS NOT NULL
#     AND u.created_at >= '{last_created_date}'
#     ORDER BY u.id ASC;
#     """

#     print("Connecting to SOURCE database...")
#     #EXTRACT
#     try:
#         src_conn = get_airflow_connection(source_db)
#         src_cur = src_conn.cursor()
#         print("Successfully connected to SOURCE database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to source DB: {e}")
#         return

#     print("Executing incremental source query...")
#     try:
#         src_cur.execute(query)
#         rows = src_cur.fetchall()
#         print(f"Query executed. New rows fetched: {len(rows)}")
#     except Exception as e:
#         print(f"[ERROR] Failed executing query: {e}")
#         src_conn.close()
#         return

#     src_conn.close()
#     print("Source DB connection closed.")

#     if not rows:
#         print("No new rows found in source database.")
#         return

#     df = pd.DataFrame(rows)
#     print(f"DataFrame created with {len(df)} new rows and {len(df.columns)} columns.")

#     #LOAD
#     print("Connecting to REPORTING database...")
#     try:
#         dest_conn = get_airflow_connection(reporting_db)
#         dest_cur = dest_conn.cursor()
#         print("Successfully connected to REPORTING database.")
#     except Exception as e:
#         print(f"[ERROR] Failed to connect to reporting DB: {e}")
#         return
    
#     table_name = "ethisx_reporting.users"
#     print(f"Ensuring table exists: {table_name}")

#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         id INT AUTO_INCREMENT PRIMARY KEY,
#         user_id INT,
#         email VARCHAR(255),
#         full_name VARCHAR(255),
#         role_id INT,
#         email_verified_at DATETIME,
#         investor_category_id INT,
#         phone_code VARCHAR(10), 
#         phone VARCHAR(50),
#         is_approved TINYINT,
#         login_with VARCHAR(50),
#         is_admin TINYINT,
#         user_created_at DATETIME,
#         user_updated_at DATETIME,
#         gender VARCHAR(20),
#         dob DATE,
#         namescan_id VARCHAR(100),
#         namescan_match_rate DECIMAL(5,2),
#         kyc_general_match_status VARCHAR(100),
#         platform_ref VARCHAR(100),
#         maala_result TEXT,
#         country VARCHAR(100),
#         user_type VARCHAR(100),
#         aml_record TEXT,
#         kyc_id INT,
#         kyc_info_id INT,
#         kyc_investor_category_id INT,
#         annual_salary VARCHAR(100),
#         net_worth VARCHAR(1000),
#         kyc_status VARCHAR(100),
#         investor_category_title VARCHAR(255),
#         investor_category_type VARCHAR(255),
#         bank_name VARCHAR(255),
#         ac_name VARCHAR(255),
#         ac_no VARCHAR(100),
#         branch VARCHAR(255),
#         swift_code VARCHAR(50),
#         iban VARCHAR(100),
#         home_address TEXT,
#         Malaa_Civil_Number VARCHAR(100),
#         Malaa_Name1 VARCHAR(255),
#         Malaa_Name2 VARCHAR(255),
#         Malaa_Name3 VARCHAR(255),
#         Malaa_LastName VARCHAR(255),
#         Malaa_Gender VARCHAR(50),
#         Malaa_Date_Of_Birth DATE,
#         Malaa_Birth_Country VARCHAR(255),
#         Malaa_Address TEXT,
#         Malaa_Mobile VARCHAR(100),
#         Malaa_Occupation VARCHAR(255),
#         Malaa_Passport_Number VARCHAR(100),
#         Malaa_Passport_Issue_Date DATE,
#         Malaa_Passport_Expiry_Date DATE,
#         UNIQUE KEY unique_user (user_id),
#         INDEX idx_user_created_at (user_created_at)
#     );
#     """
#     dest_cur.execute(create_table)

#     #BATCH INSERT
#     batch_size = 500
#     columns = [col for col in df.columns if col != "id"]   #skip id since it's AUTO_INCREMENT
#     col_names = ", ".join([f"`{col}`" for col in columns])
#     placeholders = ", ".join(["%s"] * len(columns))
#     insertion = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"

#     values = []
#     for _, row in df.reset_index(drop=True).iterrows():
#         dict_row = row.to_dict()

#         #Replace NaN with None for MySQL
#         for k, v in dict_row.items():
#             if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
#                 dict_row[k] = None

#         values.append([dict_row[col] for col in columns])

#         #Insert in batches
#         if len(values) >= batch_size:
#             dest_cur.executemany(insertion, values)
#             dest_conn.commit()
#             values.clear()

#     #Insert remaining rows
#     if values:
#         dest_cur.executemany(insertion, values)
#         dest_conn.commit()

#     dest_cur.close()
#     dest_conn.close()
#     print(f"Successfully processed {len(df)} new rows into {table_name}.")
#     print("Reporting DB connection closed.")

# # ------------------ DAG ------------------ #
# with DAG(
#     dag_id="users_airflow_incremental",
#     start_date=days_ago(1),                 #starts yesterday, so scheduler will pick it up
#     schedule_interval="0 */6 * * *",        #every 6 hours
#     catchup=False,
# ) as dag:

#     task = PythonOperator(
#         task_id="extract_and_load_incremental",
#         python_callable=extract_and_load_func,
#         execution_timeout=timedelta(minutes=30),  #increase timeout
#     )

import math
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

# Updated query to get distinct combinations and handle multiple user types
query = """SELECT DISTINCT
    u.id AS user_id, u.email, 
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.role_id, u.email_verified_at, u.investor_category_id, u.phone_code, u.phone, 
    u.is_approved, u.login_with, u.is_admin,
    u.created_at AS user_created_at, u.updated_at AS user_updated_at, u.gender, u.dob, u.namescan_id, 
    u.namescan_match_rate, u.kyc_general_match_status, u.platform_ref, u.maala_result,

    cntrs.name AS country,
    c.type AS user_type,
    a.aml_record,

    ke.id AS kyc_id, ke.kyc_info_id, ke.investor_category_id AS kyc_investor_category_id, ke.annual_salary, ke.net_worth, 
    ke.status AS kyc_status,

    ic.title AS investor_category_title,
    ic.type AS investor_category_type,

    #---------------ADMIN----------------#
    ub.bank_name, ub.ac_name, ub.ac_no, ub.branch, ub.swift_code, ub.iban, ub.home_address,

    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.CivilNumber')) AS Malaa_Civil_Number,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_1_En')) AS Malaa_Name1,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_2_En')) AS Malaa_Name2,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_3_En')) AS Malaa_Name3,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Name_6_En')) AS Malaa_LastName,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Gender_Desc_En')) AS Malaa_Gender,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Birth_DateOfBirth')) AS Malaa_Date_Of_Birth,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Birth_Country_Desc_En')) AS Malaa_Birth_Country,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Address_Wilayat_Desc_En')) AS Malaa_Address,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Current_MobileNumber')) AS Malaa_Mobile,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.Occupation_Desc_En')) AS Malaa_Occupation,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_PassportNumber')) AS Malaa_Passport_Number,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_DateOfIssue')) AS Malaa_Passport_Issue_Date,
    JSON_UNQUOTE(JSON_EXTRACT(k.maala_data, '$.PassportList[0].PassportType_DateOfExpiry')) AS Malaa_Passport_Expiry_Date

FROM ethisx_accounts.users AS u
LEFT JOIN ethisx_accounts.countries AS cntrs 
    ON u.country_id = cntrs.id
LEFT JOIN ethisx_accounts.cities AS ct
    ON u.country_id = ct.id
LEFT JOIN ethisx_accounts.user_types AS b 
    ON u.id = b.user_id
LEFT JOIN ethisx_accounts.types AS c 
    ON b.type_id = c.id
LEFT JOIN ethisx_accounts.kyc_infos AS k
    ON u.id = k.namescan_id
LEFT JOIN ethisx_accounts.kyc_ex_platforms AS ke
    ON k.id = ke.kyc_info_id
LEFT JOIN ethisx_accounts.states AS s
    ON u.id = s.id
LEFT JOIN ethisx_accounts.aml_stores AS a
    ON u.id = a.id
LEFT JOIN ethisx_accounts.investor_categories AS ic
    ON u.investor_category_id = ic.id
LEFT JOIN ethisx_accounts.user_types AS utp
    ON u.id = utp.user_id
LEFT JOIN ethisxadmin.user_bank_infos AS ub
    ON u.id = ub.user_id    
WHERE u.id IS NOT NULL
  AND u.created_at >= '2025-02-01 00:00:00'
ORDER BY u.id ASC;
"""

def get_last_processed_timestamp(**dictionary):
    """Get the last processed timestamp from the reporting database"""
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        
        # Check if table exists and get max timestamp
        check_query = """
        SELECT MAX(user_updated_at) as last_timestamp
        FROM ethisx_reporting.users
        WHERE user_updated_at IS NOT NULL
        """
        
        dest_cur.execute(check_query)
        result = dest_cur.fetchone()
        dest_conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return '2025-02-01 00:00:00'  # Default start date if no data
            
    except Exception as e:
        print(f"[INFO] Could not get last timestamp (table might not exist): {e}")
        return '2025-02-01 00:00:00'

def create_business_key_hash(row):
    """Create a business key hash to identify unique records"""
    # Combine user_id + user_type + key business fields to create unique identifier
    key_fields = [
        str(row.get('user_id', '')),
        str(row.get('user_type', '')),
        str(row.get('kyc_id', '')),
        str(row.get('kyc_info_id', ''))
    ]
    return hash('|'.join(key_fields))

#EXTRACT+LOAD
def extract_and_load_func(**dictionary):
    print("Getting last processed timestamp...")
    last_timestamp = get_last_processed_timestamp()
    print(f"Last processed timestamp: {last_timestamp}")
    
    # Update query to only get new/updated records
    incremental_query = query.replace(
        "AND u.created_at >= '2025-02-01 00:00:00'",
        f"AND (u.created_at >= '{last_timestamp}' OR u.updated_at >= '{last_timestamp}')"
    )
    
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
        src_cur.execute(incremental_query)
        rows = src_cur.fetchall()
        print(f"Query executed. Rows fetched: {len(rows)}")
    except Exception as e:
        print(f"[ERROR] Failed executing query: {e}")
        src_conn.close()
        return

    # Get column names
    column_names = [desc[0] for desc in src_cur.description]
    src_conn.close()
    print("Source DB connection closed.")

    if not rows:
        print("No new or updated rows found in source database.")
        return

    df = pd.DataFrame(rows, columns=column_names)
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
    
    table_name = "ethisx_reporting.users"
    print(f"Ensuring table exists: {table_name}")

    # Updated table schema with proper unique constraint
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        email VARCHAR(255),
        full_name VARCHAR(255),
        role_id INT,
        email_verified_at DATETIME,
        investor_category_id INT,
        phone_code VARCHAR(10), 
        phone VARCHAR(50),
        is_approved TINYINT,
        login_with VARCHAR(50),
        is_admin TINYINT,
        user_created_at DATETIME,
        user_updated_at DATETIME,
        gender VARCHAR(20),
        dob DATE,
        namescan_id VARCHAR(100),
        namescan_match_rate DECIMAL(5,2),
        kyc_general_match_status VARCHAR(100),
        platform_ref VARCHAR(100),
        maala_result TEXT,
        country VARCHAR(100),
        user_type VARCHAR(100),
        aml_record TEXT,
        kyc_id INT,
        kyc_info_id INT,
        kyc_investor_category_id INT,
        annual_salary VARCHAR(100),
        net_worth VARCHAR(1000),
        kyc_status VARCHAR(100),
        investor_category_title VARCHAR(255),
        investor_category_type VARCHAR(255),
        bank_name VARCHAR(255),
        ac_name VARCHAR(255),
        ac_no VARCHAR(100),
        branch VARCHAR(255),
        swift_code VARCHAR(50),
        iban VARCHAR(100),
        home_address TEXT,
        Malaa_Civil_Number VARCHAR(100),
        Malaa_Name1 VARCHAR(255),
        Malaa_Name2 VARCHAR(255),
        Malaa_Name3 VARCHAR(255),
        Malaa_LastName VARCHAR(255),
        Malaa_Gender VARCHAR(50),
        Malaa_Date_Of_Birth DATE,
        Malaa_Birth_Country VARCHAR(255),
        Malaa_Address TEXT,
        Malaa_Mobile VARCHAR(100),
        Malaa_Occupation VARCHAR(255),
        Malaa_Passport_Number VARCHAR(100),
        Malaa_Passport_Issue_Date DATE,
        Malaa_Passport_Expiry_Date DATE,
        business_key_hash BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_business_key (user_id, user_type, kyc_id, kyc_info_id),
        INDEX idx_user_updated_at (user_updated_at),
        INDEX idx_business_key_hash (business_key_hash)
    );
    """
    dest_cur.execute(create_table)

    # Get existing records to avoid duplicates
    print("Checking for existing records...")
    existing_query = """
    SELECT user_id, user_type, kyc_id, kyc_info_id, business_key_hash
    FROM ethisx_reporting.users
    """
    dest_cur.execute(existing_query)
    existing_records = dest_cur.fetchall()
    
    # Create set of existing business keys for fast lookup
    existing_keys = set()
    for record in existing_records:
        user_id, user_type, kyc_id, kyc_info_id, existing_hash = record
        # Create business key from the combination
        key_fields = [
            str(user_id or ''),
            str(user_type or ''),
            str(kyc_id or ''),
            str(kyc_info_id or '')
        ]
        business_key = '|'.join(key_fields)
        existing_keys.add(hash(business_key))
    
    print(f"Found {len(existing_keys)} existing unique business keys.")

    # Filter out existing records from new data
    new_rows = []
    duplicate_count = 0
    
    for _, row in df.reset_index(drop=True).iterrows():
        row_dict = row.to_dict()
        
        # Create business key for this row
        key_fields = [
            str(row_dict.get('user_id', '')),
            str(row_dict.get('user_type', '')),
            str(row_dict.get('kyc_id', '')),
            str(row_dict.get('kyc_info_id', ''))
        ]
        business_key = '|'.join(key_fields)
        business_key_hash = hash(business_key)
        
        # Check if this combination already exists
        if business_key_hash not in existing_keys:
            # Replace NaN with None for MySQL
            for k, v in row_dict.items():
                if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                    row_dict[k] = None
            
            # Add business key hash to the row
            row_dict['business_key_hash'] = business_key_hash
            new_rows.append(row_dict)
            existing_keys.add(business_key_hash)  # Add to set to avoid duplicates within this batch
        else:
            duplicate_count += 1

    print(f"Found {len(new_rows)} new records to insert.")
    print(f"Skipped {duplicate_count} duplicate records.")

    if not new_rows:
        print("No new data to insert. Exiting.")
        dest_conn.close()
        return

    # BATCH INSERT - Only new records
    batch_size = 500
    # Include business_key_hash in columns
    columns = [col for col in df.columns if col != "id"] + ['business_key_hash']
    col_names = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    
    # Use INSERT IGNORE as backup protection
    insertion = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"

    values = []
    inserted_count = 0
    
    for row_dict in new_rows:
        values.append([row_dict.get(col) for col in columns])

        # Insert in batches
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

    # Insert remaining rows
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
    total_count = dest_cur.fetchone()[0]
    
    dest_cur.close()
    dest_conn.close()
    print(f"Successfully inserted {inserted_count} new rows into {table_name}.")
    print(f"Total rows in table: {total_count}")
    print("Reporting DB connection closed.")

# Alternative approach: UPSERT (INSERT...ON DUPLICATE KEY UPDATE)
def extract_and_load_upsert(**dictionary):
    """Alternative implementation using UPSERT for better duplicate handling"""
    print("Starting UPSERT pipeline...")
    
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
        column_names = [desc[0] for desc in src_cur.description]
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

    df = pd.DataFrame(rows, columns=column_names)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

    print("Connecting to REPORTING database...")
    try:
        dest_conn = get_airflow_connection(reporting_db)
        dest_cur = dest_conn.cursor()
        print("Successfully connected to REPORTING database.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to reporting DB: {e}")
        return
    
    table_name = "ethisx_reporting.users"
    
    # Create table with proper unique constraint
    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        email VARCHAR(255),
        full_name VARCHAR(255),
        role_id INT,
        email_verified_at DATETIME,
        investor_category_id INT,
        phone_code VARCHAR(10), 
        phone VARCHAR(50),
        is_approved TINYINT,
        login_with VARCHAR(50),
        is_admin TINYINT,
        user_created_at DATETIME,
        user_updated_at DATETIME,
        gender VARCHAR(20),
        dob DATE,
        namescan_id VARCHAR(100),
        namescan_match_rate DECIMAL(5,2),
        kyc_general_match_status VARCHAR(100),
        platform_ref VARCHAR(100),
        maala_result TEXT,
        country VARCHAR(100),
        user_type VARCHAR(100),
        aml_record TEXT,
        kyc_id INT,
        kyc_info_id INT,
        kyc_investor_category_id INT,
        annual_salary VARCHAR(100),
        net_worth VARCHAR(1000),
        kyc_status VARCHAR(100),
        investor_category_title VARCHAR(255),
        investor_category_type VARCHAR(255),
        bank_name VARCHAR(255),
        ac_name VARCHAR(255),
        ac_no VARCHAR(100),
        branch VARCHAR(255),
        swift_code VARCHAR(50),
        iban VARCHAR(100),
        home_address TEXT,
        Malaa_Civil_Number VARCHAR(100),
        Malaa_Name1 VARCHAR(255),
        Malaa_Name2 VARCHAR(255),
        Malaa_Name3 VARCHAR(255),
        Malaa_LastName VARCHAR(255),
        Malaa_Gender VARCHAR(50),
        Malaa_Date_Of_Birth DATE,
        Malaa_Birth_Country VARCHAR(255),
        Malaa_Address TEXT,
        Malaa_Mobile VARCHAR(100),
        Malaa_Occupation VARCHAR(255),
        Malaa_Passport_Number VARCHAR(100),
        Malaa_Passport_Issue_Date DATE,
        Malaa_Passport_Expiry_Date DATE,
        record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_user_type_kyc (user_id, user_type, kyc_id, kyc_info_id)
    );
    """
    dest_cur.execute(create_table)

    # Get count before insertion
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_before = dest_cur.fetchone()[0]
    print(f"Records in table before insertion: {count_before}")

    # UPSERT with ON DUPLICATE KEY UPDATE
    columns = [col for col in df.columns if col != "id"]
    col_names = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    
    # Create update clause for ON DUPLICATE KEY UPDATE
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns if col not in ['user_id']])
    
    upsert_query = f"""
    INSERT INTO {table_name} ({col_names}) 
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE 
    {update_clause}
    """

    batch_size = 500
    values = []
    processed_count = 0

    for _, row in df.reset_index(drop=True).iterrows():
        dict_row = row.to_dict()

        # Replace NaN with None for MySQL
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        values.append([dict_row[col] for col in columns])

        # Insert in batches
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

    # Insert remaining rows
    if values:
        try:
            dest_cur.executemany(upsert_query, values)
            dest_conn.commit()
            processed_count += len(values)
            print(f"Processed final batch of {len(values)} rows.")
        except Exception as e:
            print(f"[ERROR] Failed to upsert final batch: {e}")
            dest_conn.rollback()

    # Get count after insertion
    dest_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_after = dest_cur.fetchone()[0]
    new_records = count_after - count_before

    dest_cur.close()
    dest_conn.close()
    
    print(f"Processing complete:")
    print(f"- Records before: {count_before}")
    print(f"- Records after: {count_after}")
    print(f"- New records added: {new_records}")
    print(f"- Total records processed: {processed_count}")
    print("Reporting DB connection closed.")

# ------------------ DAG ------------------ #
with DAG(
    dag_id="users_airflow_no_duplicates",
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",        #every 6 hours
    catchup=False,
    max_active_runs=1,  # Prevent parallel runs that could cause race conditions
) as dag:

    # Use the UPSERT approach (recommended)
    task = PythonOperator(
        task_id="extract_and_load_upsert",
        python_callable=extract_and_load_upsert,
        execution_timeout=timedelta(minutes=30),
    )
    
    # Alternative: Use the incremental approach
    # Uncomment this and comment above if you prefer the incremental approach
    # task = PythonOperator(
    #     task_id="extract_and_load_incremental",
    #     python_callable=extract_and_load_func,
    #     execution_timeout=timedelta(minutes=30),
    # )