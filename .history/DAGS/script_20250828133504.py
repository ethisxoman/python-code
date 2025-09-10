import time
import pymysql
import subprocess

db = pymysql.connect(
    host="localhost",
    user="root",
    password="yourpassword",
    database="ethisx_reporting",
    cursorclass=pymysql.cursors.DictCursor
)

while True:
    with db.cursor() as cur:
        cur.execute("SELECT id FROM dag_trigger_requests WHERE processed = 0 LIMIT 1")
        row = cur.fetchone()
        if row:
            request_id = row["id"]
            print("🚀 Triggering Airflow DAG...")
            subprocess.run(["airflow", "dags", "trigger", "withdraw_funds_pipeline"])

            cur.execute("UPDATE dag_trigger_requests SET processed = 1 WHERE id=%s", (request_id,))
            db.commit()
    time.sleep(10)  # check every 10 sec
