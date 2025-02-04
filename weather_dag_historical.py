from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Param
 
 
API_KEY = "YYHYWT2XEKVA6RLHJSFYQHDK8"
CITY = "BANGALORE" 
POSTGRES_CONN_ID = "target_postgres"
CSV_FILE_PATH = "/opt/airflow/dags/weather_data_historical.csv"
 
 
def fetch_historical_weather(**kwargs):
    start_date = kwargs["params"].get("start_date")
    end_date = kwargs["params"].get("end_date")
 
    if not start_date or not end_date:
        raise Exception("Start and end dates must be provided")
 
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/bengaluru/{start_date}/{end_date}?unitGroup=metric&include=days&key=YYHYWT2XEKVA6RLHJSFYQHDK8&contentType=json"
    response = requests.get(url)
 
    if response.status_code == 200:
        data = response.json().get("days", [])
        if not data:
            raise Exception(f"No weather data found for {start_date} to {end_date}")
        return data
    else:
        raise Exception(f"Failed to fetch weather data: {response.text}")
 
 
def store_historical_weather(**kwargs):
    data = fetch_historical_weather(**kwargs)
 
    # Use PostgresHook to get the connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
 
    upsert_query = """
    INSERT INTO weatherhistorical (city, temperature, humidity, condition, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (timestamp) 
    DO UPDATE SET 
        temperature = EXCLUDED.temperature,
        humidity = EXCLUDED.humidity,
        condition = EXCLUDED.condition;
    """
 
    for entry in data:
        values = (
            CITY,
            entry.get("temp", None),
            entry.get("humidity", None),
            entry.get("conditions", None),
            entry.get("datetime", None)
        )
        cursor.execute(upsert_query, values)
 
    conn.commit()
    cursor.close()
    conn.close()
    print(data)
    print("Weather data successfully stored in PostgreSQL.")
 
 
def export_weather_data_to_csv(**kwargs):
    start_date = kwargs["params"].get("start_date")
    end_date = kwargs["params"].get("end_date")
 
    if not start_date or not end_date:
        raise Exception("Start and end dates must be provided")
 
    # Use PostgresHook to get the connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
 
    cursor.execute("""
        SELECT city, temperature, humidity, condition, timestamp 
        FROM weatherhistorical 
        WHERE timestamp BETWEEN %s AND %s;
    """, (start_date, end_date))
 
    rows = cursor.fetchall()
 
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
 
    with open(CSV_FILE_PATH, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["City", "Temperature", "Humidity", "Condition", "Timestamp"])  # CSV Header
 
        for row in rows:
            writer.writerow(row)
 
    cursor.close()
    conn.close()
    print(f"Weather data successfully exported to CSV: {CSV_FILE_PATH}")
 
 
with DAG(
    "weather_pipeline_historical",
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
