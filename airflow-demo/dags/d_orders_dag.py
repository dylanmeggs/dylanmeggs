from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime, timedelta
from d_orders.extract.d_orders_extract import extract_orders_to_s3

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False, #We can run today's workflow even if yesterday's failed
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="d_orders_pipeline",
    default_args=default_args,
    description="Extract, transform, load for d_orders dimension",
    start_date=datetime(2025, 11, 1),
    # Run daily at 8am PT
    # Check crontab.guru for a visual representation on how scheduling works
    # Or put schedule_interval="@daily" if timing doesn't matter
    schedule_interval="0 8 * * *",  
    timezone="America/Los_Angeles", # Exclude this if you want default UTC and don't care about daylight savings
    catchup=False,
    tags=["orders", "dim", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_orders_to_s3",
        # Connection defined in Airflow’s Connections UI, or in MWAA environment variables 
        # Will point to Glue or Lambda
        python_callable=extract_orders_to_s3, 
    )

    transform = RedshiftSQLOperator(
        task_id="transform_orders",
        sql="transform/d_orders_transform.sql",
        # Tell Airflow where to look when the SQL runs
        # I define the my_redshift_conn connection myself in Airflow’s Connections UI, or in MWAA environment variables 
        # It contains: Hostname (endpoint of Redshift cluster) + Port (usually 5439) + Database name + Username/Password or IAM role
        redshift_conn_id="my_redshift_conn", 
    )

    load = RedshiftSQLOperator(
        task_id="load_orders",
        sql="load/d_orders_load.sql",
        redshift_conn_id="my_redshift_conn",
    )

    cleanup = RedshiftSQLOperator(
        task_id="cleanup_staging",
        sql="cleanup/d_orders_cleanup.sql",
        redshift_conn_id="my_redshift_conn",
    )

    extract >> transform >> load >> cleanup