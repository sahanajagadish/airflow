import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Param
from utils import store_historical_weather, export_weather_data_to_csv
from dotenv import load_dotenv

load_dotenv()

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "target_postgres")

with DAG(
    "weather_pipeline_historical_util",
    schedule_interval="@daily",
    default_args={"owner": "airflow", "start_date": days_ago(1)},
    catchup=False,
    params={  
        "start_date": Param("2024-01-01", type="string"),
        "end_date": Param("2025-01-31", type="string"),
    },
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS weatherhistorical (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT,
            humidity INT,
            condition TEXT,
            timestamp DATE UNIQUE
        );
        """,
    )

    store_data_task = PythonOperator(
        task_id="store_weather_data",
        python_callable=store_historical_weather,
        provide_context=True,
    )

    export_to_csv_task = PythonOperator(
        task_id="export_weather_data_to_csv",
        python_callable=export_weather_data_to_csv,
        provide_context=True,
    )

    create_table >> store_data_task >> export_to_csv_task
