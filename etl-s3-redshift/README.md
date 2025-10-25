# End-to-End ETL: Airflow → S3 → Redshift (Staging → MERGE)

This project stands up Apache Airflow with a DAG that:
1) Generates orders data (extract),
2) Cleans/transforms with Pandas,
3) Uploads to S3 in a partitioned path (dt=YYYYMMDD),
4) COPYs into Redshift **staging**,
5) **MERGEs** into a target table (idempotent upsert),
6) Truncates staging.

## Prereqs
- Docker + Docker Compose
- AWS S3 bucket + Redshift cluster (network accessible from your machine / docker host)
- (Optional) IAM policy allowing S3 read for Redshift COPY

## Quick Start

1. Copy env and fill it:
   ```bash
   cp .env.example .env
   # edit .env (AWS keys, S3 bucket, Redshift conn)
   ```

2. Start services:
   ```bash
   docker compose up -d postgres redis
   docker compose up airflow-init
   docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer
   ```

3. Open Airflow UI: http://localhost:8080  
   - Login: `admin` / `admin`
   - Ensure Connections:
     - `aws_default` (if needed; otherwise env keys are used)
     - `redshift_default` (set by env var or create via UI)
   - Variable: `S3_DATA_BUCKET` = your S3 bucket name

4. Unpause DAG: `etl_s3_to_redshift_orders`  
   Trigger a run. Check task logs.

## Talking Points (Interview)
- Partitioned S3 layout: `orders/dt=YYYYMMDD/orders.csv`
- Idempotent loads: staging → MERGE → truncate staging
- Data quality: null/duplicate handling, typed timestamps
- Simple extension: switch CSV→Parquet, add Great Expectations, add Slack alerts

## Cleanup
- `docker compose down -v`
