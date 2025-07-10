from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='create_mssql_table_dag',
    default_args=default_args,
    start_date=datetime(2025, 5, 4),
    schedule_interval=None,
    catchup=False,
    tags=['mssql', 'example'],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table_task',
        conn_id='mssql_odbc',  # <-- Twój ODBC connection ID
        sql="""
            CREATE TABLE ExampleTable (
                id INT PRIMARY KEY IDENTITY(1,1),
                name VARCHAR(100),
                created_at DATETIME DEFAULT GETDATE()
            );
        """,
        autocommit=True,
    )
###sadzasdasd