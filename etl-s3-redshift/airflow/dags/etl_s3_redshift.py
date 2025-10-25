from __future__ import annotations

import io
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Amazon / Redshift providers
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# -----------------------------
# CONFIG (edit to your env)
# -----------------------------
AWS_CONN_ID = "aws_default"               # Or rely on AWS_* env vars without a connection
REDSHIFT_CONN_ID = "redshift_default"     # Connection string set by env or UI
S3_BUCKET = Variable.get("S3_DATA_BUCKET")  # Set Airflow Variable: S3_DATA_BUCKET
S3_PREFIX = "orders"                      # Folder/prefix in S3
REDSHIFT_SCHEMA = "public"
REDSHIFT_STAGING_TABLE = "stg_orders"
REDSHIFT_TARGET_TABLE  = "dim_orders"

# -----------------------------
# SQL
# -----------------------------
DDL_SQL = f"""
CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{REDSHIFT_STAGING_TABLE} (
    order_id       BIGINT   ENCODE az64,
    customer_id    BIGINT   ENCODE az64,
    order_ts       TIMESTAMP,
    amount         DECIMAL(12,2),
    status         VARCHAR(32),
    _ingest_run_at TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{REDSHIFT_TARGET_TABLE} (
    order_id        BIGINT   PRIMARY KEY,
    customer_id     BIGINT,
    order_ts        TIMESTAMP,
    amount          DECIMAL(12,2),
    status          VARCHAR(32),
    last_updated_at TIMESTAMP DEFAULT GETDATE()
);
"""

MERGE_SQL = f"""
MERGE INTO {REDSHIFT_SCHEMA}.{REDSHIFT_TARGET_TABLE} AS t
USING (
    SELECT order_id, customer_id, order_ts, amount, status
    FROM {REDSHIFT_SCHEMA}.{REDSHIFT_STAGING_TABLE}
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id     = s.customer_id,
    order_ts        = s.order_ts,
    amount          = s.amount,
    status          = s.status,
    last_updated_at = GETDATE()
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_ts, amount, status, last_updated_at)
VALUES (s.order_id, s.customer_id, s.order_ts, s.amount, s.status, GETDATE());
"""

TRUNCATE_STAGING_SQL = f"TRUNCATE TABLE {REDSHIFT_SCHEMA}.{REDSHIFT_STAGING_TABLE};"

# -----------------------------
# Python Callables
# -----------------------------
def extract_orders_to_dataframe(**context):
    """
    Simulates extraction by generating deterministic 'orders' data for the run date.
    Replace with your real API/DB extraction.
    """
    ds = context["ds"]  # 'YYYY-MM-DD' Airflow execution date string
    rng = np.random.default_rng(abs(hash(ds)) % (2**32))

    n = 500
    df = pd.DataFrame({
        "order_id":     np.arange(1, n + 1),
        "customer_id":  rng.integers(1000, 2000, size=n),
        "order_ts":     pd.to_datetime(ds) + pd.to_timedelta(rng.integers(0, 86400, size=n), unit="s"),
        "amount":       np.round(rng.uniform(5, 500, size=n), 2),
        "status":       rng.choice(["placed", "paid", "shipped", "cancelled"], size=n, p=[0.4, 0.35, 0.2, 0.05])
    })

    # Add duplicates/dirty rows on purpose (to show cleaning)
    dirty = df.sample(frac=0.05, random_state=42)
    df_dirty = pd.concat([df, dirty], ignore_index=True)

    context["ti"].xcom_push(key="raw_orders_json", value=df_dirty.to_json(orient="records", date_format="iso"))

def transform_and_upload_to_s3(**context):
    """
    Cleans data and uploads a CSV to partitioned S3:
      s3://<bucket>/orders/dt={{ ds_nodash }}/orders.csv
    """
    ti = context["ti"]
    raw_json = ti.xcom_pull(key="raw_orders_json", task_ids="extract_orders")

    df = pd.read_json(io.StringIO(raw_json), orient="records")

    # Sanitization
    df["order_ts"] = pd.to_datetime(df["order_ts"], errors="coerce")
    df = df.dropna(subset=["order_id", "customer_id", "order_ts", "amount"])
    df = df[df["amount"] > 0]
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    # Reorder columns (explicit schema)
    df = df[["order_id", "customer_id", "order_ts", "amount", "status"]]

    # Serialize CSV in-memory
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    csv_bytes = io.BytesIO(csv_buf.getvalue().encode("utf-8"))

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_key = f"{S3_PREFIX}/dt={context['ds_nodash']}/orders.csv"
    s3.load_file_obj(
        file_obj=csv_bytes,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    ti.xcom_push(key="s3_key", value=s3_key)
    ti.xcom_push(key="row_count", value=len(df))

# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_s3_to_redshift_orders",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "s3", "redshift", "demo"],
) as dag:

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders_to_dataframe,
        provide_context=True,
    )

    transform_upload = PythonOperator(
        task_id="transform_upload",
        python_callable=transform_and_upload_to_s3,
        provide_context=True,
    )

    create_tables = RedshiftSQLOperator(
        task_id="create_redshift_tables",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=DDL_SQL,
    )

    # COPY into staging table from S3 partition for current run
    s3_to_redshift = S3ToRedshiftOperator(
        task_id="copy_to_redshift_staging",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_STAGING_TABLE,
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}/dt={{ ds_nodash }}/orders.csv",
        copy_options=[
            "CSV",
            "IGNOREHEADER 1",
            "TIMEFORMAT auto",
            "TRUNCATECOLUMNS",
            "BLANKSASNULL",
            "EMPTYASNULL",
            "COMPUPDATE OFF"
        ],
        method="REPLACE",  # Truncate staging before COPY
        include_header=True,
    )

    merge_upsert = RedshiftSQLOperator(
        task_id="merge_upsert_target",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=MERGE_SQL,
    )

    truncate_staging = RedshiftSQLOperator(
        task_id="truncate_staging",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=TRUNCATE_STAGING_SQL,
    )

    extract_orders >> transform_upload >> create_tables >> s3_to_redshift >> merge_upsert >> truncate_staging
