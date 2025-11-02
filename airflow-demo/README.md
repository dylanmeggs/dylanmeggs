# Airflow Demo: Redshift ETL Pipeline

This repository contains a simple Airflow DAG demonstrating an ETL workflow:
1. Logs the start of the job
2. Runs an extraction SQL step using an API call which dumps data into AWS S3
3. Transforms and loads the data
4. Deletes the staging table used in the transform (see notes for use case and alternatives)
5. Logs job completion

Concepts shown:
- DAG structure (`with DAG(...)`)
- Task dependencies (`task_1 >> task_2`)
- Use of Airflow Operators (`PythonOperator`, `RedshiftSQLOperator`)
- Parameterized scheduling and metadata (`start_date`, `catchup`, etc.)
- Use of AWS Secrets Manager and OAuth token authentication
- Merge Upserting with Redshift vs Other SQL Engines

In this example, I assume we're storing Airflow with MWAA and DAGs are stored in S3

To run locally:
pip install apache-airflow
airflow standalone

Then add this DAG to the `dags/` folder and open `localhost:8080`