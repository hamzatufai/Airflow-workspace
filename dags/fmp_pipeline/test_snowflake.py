from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="snowflake_connection_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Updated for modern Airflow
    catchup=False,
) as dag:

    run_snowflake_query = SQLExecuteQueryOperator(
        task_id="execute_query",
        conn_id="snowflake_default",  # Matches your Airflow Connection Id
        sql="SELECT CURRENT_VERSION();",
    )

    run_snowflake_query
