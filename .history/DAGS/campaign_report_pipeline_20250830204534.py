# from datetime import datetime
# import math
# import mysql.connector
# import pandas as pd
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# from connection import source_db, reporting_db

# print("Starting pipeline script...")

# query = """SELECT 
# c.id AS campaign_id, c.`order`, c.title, c.short_desc, c.user_id, c.progress_type_id,  
# c.start_date, c.end_date, c.min_amount, c.max_amount, c.project_timeline,
# c.max_predefined_amount, c.min_predefined_amount, c.funding_goal, c.currency, 
# c.max_equity_offered, c.min_equity_offered, c.divident_target, c.roi_after_tax, 
# c.irr_after_tax, c.recommended_amount, c.contributor_table, c.location, c.country_id, 
# c.admin_aproval, c.draft, c.approved_by, c.approved_at, c.timeline_start_date, 
# c.timeline_end_date, c.payout_notify, c.created_at, c.deleted_at, c.updated_at, 
# c.campaign_type, c.campaign_end_method, c.campaign_opened_at, c.campaign_closed_at, 
# c.status, c.is_public, c.is_private, c.meta_title, c.campaign_close_request,
# ut.type_id as user_type_id
# FROM ethisx_admin.campaign_masters AS c
# LEFT JOIN ethisx_accounts.user_types AS ut 
#     ON c.user_id = ut.user_id
# WHERE c.id IS NOT NULL
# ORDER BY c.id;
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

#     table_name = "ethisx_reporting.campaign"
#     print(f"Ensuring table exists: {table_name}")

#     create_table = f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         id INT AUTO_INCREMENT PRIMARY KEY,
#         campaign_id INT NOT NULL,
#         `order` INT,
#         title VARCHAR(500),
#         short_desc TEXT,
#         user_id INT,
#         progress_type_id INT,
#         start_date DATETIME,
#         end_date DATETIME,
#         min_amount DECIMAL (18,2),
#         max_amount DECIMAL (18,2),
#         project_timeline VARCHAR(255),
#         max_predefined_amount DECIMAL (18,2),
#         min_predefined_amount DECIMAL(18,2),
#         funding_goal DECIMAL(18,2),
#         currency VARCHAR(50),
#         max_equity_offered DECIMAL(10,2),
#         min_equity_offered DECIMAL(10,2),
#         divident_target DECIMAL(10,2),
#         roi_after_tax DECIMAL(10,2),
#         irr_after_tax DECIMAL(10,2),
#         recommended_amount DECIMAL(18,2),
#         contributor_table VARCHAR(255),
#         location VARCHAR(255),
#         country_id INT,
#         admin_aproval TINYINT(1),
#         draft TINYINT(1),
#         approved_by INT,
#         approved_at DATETIME,
#         timeline_start_date DATETIME,
#         timeline_end_date DATETIME,
#         payout_notify TINYINT(1),
#         created_at DATETIME,
#         deleted_at DATETIME,
#         updated_at DATETIME,
#         campaign_type VARCHAR(100),
#         campaign_end_method VARCHAR(100),
#         campaign_opened_at DATETIME,
#         campaign_closed_at DATETIME,
#         status VARCHAR(50),
#         is_public TINYINT(1),
#         is_private TINYINT(1),
#         meta_title VARCHAR(500),
#         campaign_close_request TINYINT,
#         user_type_id INT,
#         UNIQUE KEY unique_campaign (campaign_id)
#     );
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

# # #DAG
# # with DAG(
# #     dag_id="campaign_report_pipeline",
# #     start_date=datetime(2025, 8, 20),
# #     schedule_interval="@daily",
# #     catchup=False
# # ) as dag:

# #     task = PythonOperator(
# #         task_id="extract_and_load",
# #         python_callable=extract_and_load_func,
# #         provide_context=True
# #     )

# if __name__ == "__main__":
#     print("Running in standalone mode...")
#     extract_and_load_func()
#-----------------------------------------------------------------

from datetime import datetime
import math
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """SELECT 
c.id AS campaign_id, c.`order`, c.title, c.short_desc, c.user_id, c.progress_type_id,  
c.start_date, c.end_date, c.min_amount, c.max_amount, c.project_timeline,
c.max_predefined_amount, c.min_predefined_amount, c.funding_goal, c.currency, 
c.max_equity_offered, c.min_equity_offered, c.divident_target, c.roi_after_tax, 
c.irr_after_tax, c.recommended_amount, c.contributor_table, c.location, c.country_id, 
c.admin_aproval, c.draft, c.approved_by, c.approved_at, c.timeline_start_date, 
c.timeline_end_date, c.payout_notify, c.created_at, c.deleted_at, c.updated_at, 
c.campaign_type, c.campaign_end_method, c.campaign_opened_at, c.campaign_closed_at, 
c.status, c.is_public, c.is_private, c.meta_title, c.campaign_close_request,
ut.type_id as user_type_id
FROM ethisxadmin.campaign_masters AS c
LEFT JOIN ethisx_accounts.user_types AS ut 
    ON c.user_id = ut.user_id
WHERE c.id IS NOT NULL
ORDER BY c.id;
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

    table_name = "ethisx_reporting.campaign"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        campaign_id INT NOT NULL,
        `order` INT,
        title VARCHAR(500),
        short_desc TEXT,
        user_id INT,
        progress_type_id INT,
        start_date DATETIME,
        end_date DATETIME,
        min_amount DECIMAL (18,2),
        max_amount DECIMAL (18,2),
        project_timeline VARCHAR(255),
        max_predefined_amount DECIMAL (18,2),
        min_predefined_amount DECIMAL(18,2),
        funding_goal DECIMAL(18,2),
        currency VARCHAR(50),
        max_equity_offered DECIMAL(10,2),
        min_equity_offered DECIMAL(10,2),
        divident_target DECIMAL(10,2),
        roi_after_tax DECIMAL(10,2),
        irr_after_tax DECIMAL(10,2),
        recommended_amount DECIMAL(18,2),
        contributor_table VARCHAR(255),
        location VARCHAR(255),
        country_id INT,
        admin_aproval TINYINT(1),
        draft TINYINT(1),
        approved_by INT,
        approved_at DATETIME,
        timeline_start_date DATETIME,
        timeline_end_date DATETIME,
        payout_notify TINYINT(1),
        created_at DATETIME,
        deleted_at DATETIME,
        updated_at DATETIME,
        campaign_type VARCHAR(100),
        campaign_end_method VARCHAR(100),
        campaign_opened_at DATETIME,
        campaign_closed_at DATETIME,
        status VARCHAR(50),
        is_public TINYINT(1),
        is_private TINYINT(1),
        meta_title VARCHAR(500),
        campaign_close_request TINYINT,
        user_type_id INT,
        UNIQUE KEY unique_campaign (campaign_id)
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
    schedule_interval="@daily",   
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
    )