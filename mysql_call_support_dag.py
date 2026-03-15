from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import duckdb
import logging

DUCKDB_PATH = '/usr/local/airflow/data/support_analytics.duckdb'
JSON_DIR = '/usr/local/airflow/data/telephony_data/'

default_args = {
    "owner": "airflow",
    "retries": 2, 
    "retry_delay": timedelta(minutes=5), # add retries
}

@dag(
    dag_id="taskflow_support_call_enrichment_v2",
    default_args=default_args,
    start_date=datetime(2024, 3, 1),
    schedule="@hourly",
    catchup=False
)
def support_call_pipeline():

    @task(task_id="extract_mysql")
    def detect_new_calls() -> dict:
        last_loaded = Variable.get("last_loaded_call_time", default_var="2000-01-01 00:00:00")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_support_db')
        
        sql = f"SELECT * FROM calls WHERE call_time > '{last_loaded}'"
        new_calls_df = mysql_hook.get_pandas_df(sql)
        
        # logging
        logging.info(f"Found {len(new_calls_df)} new calls since {last_loaded}")

        if new_calls_df.empty:
            return {"call_ids": [], "new_watermark": last_loaded, "raw_calls_data": []}
        
        new_watermark = str(new_calls_df['call_time'].max())
        call_ids = new_calls_df['call_id'].tolist()
        new_calls_df['call_time'] = new_calls_df['call_time'].astype(str)

        return {
            "call_ids": call_ids, 
            "new_watermark": new_watermark,
            "raw_calls_data": new_calls_df.to_dict('records')
        }

    @task(task_id="extract_json")
    def load_telephony_details(new_calls_info: dict) -> list[dict]:
        call_ids = new_calls_info.get("call_ids", [])
        telephony_data = []
        rejected_count = 0
        
        for call_id in call_ids:
            file_path = os.path.join(JSON_DIR, f"{call_id}.json")
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    # data quality check
                    if data.get("duration_sec", -1) >= 0:
                        telephony_data.append(data)
                    else:
                        logging.warning(f"Rejected call_id {call_id}: Negative duration.")
                        rejected_count += 1
            except FileNotFoundError:
                logging.warning(f"JSON missing for call_id {call_id}.")
                rejected_count += 1
        
        # observability log
        logging.info(f"Successfully loaded {len(telephony_data)} JSONs. Rejected/Missing: {rejected_count}")
        return telephony_data

    @task(task_id="transform_and_load_duckdb")
    def transform_and_load(new_calls_info: dict, telephony_data: list) -> None:
        if not new_calls_info or not new_calls_info.get("call_ids"):
            return
        
        raw_calls_df = pd.DataFrame(new_calls_info.get("raw_calls_data"))
        telephony_df = pd.DataFrame(telephony_data)

        mysql_hook = MySqlHook(mysql_conn_id='mysql_support_db')
        employees_df = mysql_hook.get_pandas_df("SELECT employee_id, full_name, team FROM employees")

        conn = duckdb.connect(DUCKDB_PATH)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS support_call_enriched (
                call_id INTEGER PRIMARY KEY,
                employee_name VARCHAR,
                team VARCHAR,
                call_time TIMESTAMP,
                phone VARCHAR,
                direction VARCHAR,
                status VARCHAR,
                duration_sec INTEGER,
                short_description VARCHAR
            )
        """)

        # get the row count for logging
        logging.info(f"Finalizing upsert for {len(raw_calls_df)} enriched records.")

        upsert_sql = """
            INSERT INTO support_call_enriched
            SELECT 
                CAST(c.call_id AS INTEGER),
                e.full_name, e.team, CAST(c.call_time AS TIMESTAMP),
                c.phone, c.direction, c.status,
                CAST(t.duration_sec AS INTEGER), t.short_description
            FROM raw_calls_df c
            JOIN employees_df e ON c.employee_id = e.employee_id
            LEFT JOIN telephony_df t ON c.call_id = t.call_id
            ON CONFLICT (call_id) DO UPDATE SET
                status = EXCLUDED.status,
                duration_sec = EXCLUDED.duration_sec;
        """
        conn.execute(upsert_sql)
        conn.close()

        Variable.set("last_loaded_call_time", new_calls_info.get("new_watermark"))
        logging.info("Pipeline completed successfully.")

    calls_info = detect_new_calls()
    telephony_info = load_telephony_details(calls_info)
    transform_and_load(calls_info, telephony_info)

support_call_pipeline()
