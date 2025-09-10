from datetime import datetime
import math
import mysql.connector
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from connection import source_db, reporting_db

print("Starting user report pipeline script...")
query = """SELECT distinct
    u.id AS user_id, u.email, 
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.role_id,  u.email_verified_at, u.investor_category_id, u.phone_code, u.phone, 
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

#-------------ACCOUNTS---------------#
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

#-----------ADMIN--------------#
LEFT JOIN ethisxadmin.user_bank_infos AS ub
	ON u.id = ub.user_id    

WHERE u.id IS NOT NULL
ORDER BY u.id ASC;
"""

#extract and load

def extract_and_load_func(**dictionary):
    print ('extracting database...')
    
    #extracting from source
    src_conn = mysql.connector.connect(**source_db)
    src_cur = src_conn.cursor(dictionary=True)
    src_cur.execute(query)
    rows = src_cur.fetchall()
    src_conn.close()

    if not rows:
        print("No rows found in source database.")
        return

    df = pd.DataFrame(rows)

    #inserting directly into destination
    dest_conn = mysql.connector.connect(**reporting_db)
    dest_cur = dest_conn.cursor()

    table_name = "ethisx_reporting.users"

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
        UNIQUE KEY unique_user (id, user_id)
    );
    """
    dest_cur.execute(create_table)

    for _, row in df.iterrows():
        dict_row = row.to_dict()

        #replacing NaN with None for MySQL compatibility
        for k, v in dict_row.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                dict_row[k] = None

        columns = list(dict_row.keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        # insertion = f"""
        # REPLACE INTO {table_name} ({col_names})
        # VALUES ({placeholders})
        # """
        # dest_cur.execute(insertion, list(dict_row.values()))
        insertion = f"""
        INSERT IGNORE INTO {table_name} ({col_names})
        VALUES ({placeholders})
        """  
        dest_cur.execute(insertion, list(dict_row.values()))
        
    dest_conn.commit()
    dest_cur.close()
    dest_conn.close()
    print(f"Inserted/updated {len(df)} rows into {table_name}.")

# DAG Definition
dag = DAG(
    dag_id="user_report_pipeline",
    start_date=datetime(2025, 8, 25),
    schedule_interval="* * * * *",  # every minute
    catchup=False
)

task = PythonOperator(
    task_id="extract_and_load",
    python_callable=extract_and_load_func,
    dag=dag
)

