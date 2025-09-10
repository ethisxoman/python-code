import math
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from connection import get_airflow_connection, source_db, reporting_db

print("Starting pipeline script...")

query = """SELECT 
i.issuer_id, 
b.id as campaign_id,
#---issuer---
i.issuer_type, i.email, i.phone_code, i.phone_no, i.application_type, i.fnc, i.crn, i.co, i.nbi, i.cpbd, i.cc, i.cpuc, i.min_fag, i.max_fag, 
i.bdbdypuf, i.lar, i.lani, i.dcp, i.pt,  i.nas, i.c_addr, i.business_activities, i.company_type, i.wldypc, i.hdyae, i.ihdccetc, i.ajc_ecf, i.ajc_p2p, 
i.cbl, i.ci, i.gambling, i.llra, i.ppra, i.nhfb, i.snce, i.ttra, i.iicai, i.dsnci, i.oadcaspd, i.tscrp, i.trpcg, i.share_t, i.stock_bb, i.rrsca, 
i.oadncaspd, i.tsncrp, i.ba_trpcg, i.total_a, i.tcsti, i.sncsstii, i.t_debt, i.sncdi, i.jurisdictional_delaration, i.df, i.itapullrgcoipss, 
i.iaaosptcrllralra, i.sfridspvydsmdabq, i.afsqaateaullreeci, i.werallraocpcbbocpospi, i.gldr, i.lrfcafoc, i.tmpfoac, i.trfievorrsic, i.tsdrcj, 
i.icacsafsr, i.atalcfer, i.aftijfojp, i.atarpoafpdhjraptfo, i.wcssi, i.declaration, i.foreign_ownship, i.fa_afs_lstt, i.fa_afs_mrtt, i.itbs, 
i.obligation, i.transparancy, i.weisde_taohl, i.iccne, i.tciiicp_usd, i.tams, i.agr_cmi, i.ip_oic, i.pfc, i.atarftjioj, i.operation_key_management, 
i.min_rev, i.shareholder_wicl, i.bank_ac_wicl, i.public_lcco, i.bounce_cmfr, i.cur_litigation_apac, i.shdircom, i.negative_equity_bl, i.risk_scoring, 
i.no_current_dispute, i.project_fscfi, i.clear_role_pos, i.completed_project_fpf, i.future_payment_fpf, i.company_awarded_payor, i.completed_project_fif, 
i.future_payment_fif, i.disclose_srr, i.asset_dnuc, i.on_boarding, i.dd_ongoing, i.ic_approval, i.status AS "on_boarding_status", 
i.submission_date, i.pre_campaign_fee, i.created_at, i.updated_at

FROM ethisxadmin.issuer_onboardings AS i
left join ethisxadmin.campaign_masters as b 
on i.issuer_id = b.user_id
WHERE i.issuer_id IS NOT NULL
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

    table_name = "ethisx_reporting.issuer_onboardings"
    print(f"Ensuring table exists: {table_name}")

    create_table = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    issuer_id INT NOT NULL,
    issuer_type VARCHAR(50),
    email VARCHAR(255),
    phone_code VARCHAR(10),
    phone_no VARCHAR(15),
    application_type VARCHAR(50),
    fnc TEXT,
    crn TEXT,
    co TEXT,
    nbi TEXT,
    cpbd TEXT,
    cc TEXT,
    cpuc VARCHAR(50),
    min_fag DECIMAL(18, 2),
    max_fag DECIMAL(18, 2),
    bdbdypuf VARCHAR(50),
    lar VARCHAR(50),
    lani VARCHAR(50),
    dcp VARCHAR(50),
    pt TEXT,
    nas VARCHAR(255),
    c_addr TEXT,
    business_activities TEXT,
    company_type VARCHAR(50),
    wldypc VARCHAR(50),
    hdyae VARCHAR(50),
    ihdccetc VARCHAR(50),
    ajc_ecf VARCHAR(50),
    ajc_p2p VARCHAR(50),
    cbl VARCHAR(50),
    ci VARCHAR(50),
    gambling VARCHAR(50),
    llra VARCHAR(50),
    ppra VARCHAR(50),
    nhfb VARCHAR(50),
    snce VARCHAR(50),
    ttra VARCHAR(50),
    iicai VARCHAR(50),
    dsnci VARCHAR(50),
    oadcaspd VARCHAR(50),
    tscrp VARCHAR(50),
    trpcg VARCHAR(50),
    share_t VARCHAR(50),
    stock_bb VARCHAR(50),
    rrsca VARCHAR(50),
    oadncaspd VARCHAR(50),
    tsncrp VARCHAR(50),
    ba_trpcg VARCHAR(50),
    total_a DECIMAL(18, 2),
    tcsti DECIMAL(18, 2),
    sncsstii DECIMAL(18, 2),
    t_debt DECIMAL(18, 2),
    sncdi VARCHAR(50),
    jurisdictional_delaration TEXT,
    df VARCHAR(50),
    itapullrgcoipss VARCHAR(50),
    iaaosptcrllralra VARCHAR(50),
    sfridspvydsmdabq VARCHAR(50),
    afsqaateaullreeci VARCHAR(50),
    werallraocpcbbocpospi VARCHAR(50),
    gldr VARCHAR(50),
    lrfcafoc VARCHAR(50),
    tmpfoac VARCHAR(50),
    trfievorrsic VARCHAR(50),
    tsdrcj VARCHAR(50),
    icacsafsr VARCHAR(50),
    atalcfer VARCHAR(50),
    aftijfojp VARCHAR(50),
    atarpoafpdhjraptfo VARCHAR(50),
    wcssi VARCHAR(50),
    declaration VARCHAR(50),
    foreign_ownship VARCHAR(50),
    fa_afs_lstt VARCHAR(50),
    fa_afs_mrtt VARCHAR(50),
    itbs VARCHAR(50),
    obligation VARCHAR(50),
    transparancy VARCHAR(50),
    weisde_taohl VARCHAR(50),
    iccne VARCHAR(50),
    tciiicp_usd DECIMAL(18, 2),
    tams VARCHAR(50),
    agr_cmi VARCHAR(50),
    ip_oic VARCHAR(50),
    pfc VARCHAR(50),
    atarftjioj VARCHAR(50),
    operation_key_management VARCHAR(50),
    min_rev DECIMAL(18, 2),
    shareholder_wicl VARCHAR(50),
    bank_ac_wicl VARCHAR(50),
    public_lcco VARCHAR(50),
    bounce_cmfr VARCHAR(50),
    cur_litigation_apac VARCHAR(50),
    shdircom VARCHAR(50),
    negative_equity_bl VARCHAR(50),
    risk_scoring VARCHAR(50),
    no_current_dispute VARCHAR(50),
    project_fscfi VARCHAR(50),
    clear_role_pos VARCHAR(50),
    completed_project_fpf VARCHAR(50),
    future_payment_fpf VARCHAR(50),
    company_awarded_payor VARCHAR(50),
    completed_project_fif VARCHAR(50),
    future_payment_fif VARCHAR(50),
    disclose_srr VARCHAR(50),
    asset_dnuc VARCHAR(50),
    on_boarding VARCHAR(50),
    dd_ongoing VARCHAR(50),
    ic_approval VARCHAR(50),
    on_boarding_status VARCHAR(50),
    submission_date DATETIME,
    pre_campaign_fee DECIMAL(18, 2),
    created_at DATETIME,
    updated_at DATETIME, 
    UNIQUE KEY unique_issuer (campaign_id, issuer_id)
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
    dag_id="issuer_onboardings_airflow",
    start_date=days_ago(1),      #starts yesterday, so scheduler will pick it up
    schedule_interval="0 */6 * * *",   
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_func,
    )
