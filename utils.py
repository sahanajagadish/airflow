import os
import requests
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
CITY = os.getenv("CITY", "BANGALORE")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "target_postgres")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/opt/airflow/dags/weather_data_historical.csv")

def fetch_historical_weather(**kwargs):
    start_date = kwargs["params"].get("start_date")
    end_date = kwargs["params"].get("end_date")

    if not start_date or not end_date:
        raise Exception("Start and end dates must be provided")

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}/{start_date}/{end_date}?unitGroup=metric&include=days&key={API_KEY}&contentType=json"
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
    print("Weather data successfully stored in PostgreSQL.")

def export_weather_data_to_csv(**kwargs):
    start_date = kwargs["params"].get("start_date")
    end_date = kwargs["params"].get("end_date")

    if not start_date or not end_date:
        raise Exception("Start and end dates must be provided")

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
