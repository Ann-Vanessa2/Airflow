from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

API_CONN_ID = 'weather_api_conn_id'
POSTGRES_CONN_ID = 'postgres_conn_id'

weather_connection = BaseHook.get_connection(API_CONN_ID)
api_key = weather_connection.extra_dejson.get('api_key')

default_args = {
    'owner': 'Ann',
    'start_date': datetime(2024, 11, 28),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'lab5',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['labs']
) as dag:

    # check if the weather API is online    
    check_api_task = HttpSensor(
        task_id='check_api',
        http_conn_id=API_CONN_ID,
        endpoint=f'/data/2.5/weather?q=Portland&APPID={api_key}',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=60
    )
    
    # fetch the current weather data for a specific city
    def fetch_weather_data():
        url = f'https://api.openweathermap.org/data/2.5/weather?q=Portland&appid={api_key}'
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception('Failed to fetch weather data')
        data = response.json()
        return data
    
    fetch_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    # transform the weather data into more readable format by converting from Kelvin to Fahrenheit and adding timestamps to local time
    def transform_weather_data(ti):
        weather_data = ti.xcom_pull(task_ids='fetch_weather_data')
        kelvin_temp = weather_data['main']['temp']
        fahrenheit_temp = (kelvin_temp - 273.15) * 1.8 + 32
        pressure = weather_data['main']['pressure']
        humidity = weather_data['main']['humidity']
        city = weather_data['name']
        # Converting timestamps to local timezone
        from datetime import timezone
        timestamp = datetime.fromtimestamp(weather_data['dt'], tz=timezone.utc)
        return {
            'temp': fahrenheit_temp,
            'pressure': pressure,
            'humidity': humidity,
            'city': city,
            'timestamp': timestamp,
        }
    
    transform_data_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
    )


    def load_weather_data(ti):
        transform_data = ti.xcom_pull(task_ids='transform_weather_data')
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # CREATE SCHEMA 
        cursor.execute('CREATE SCHEMA IF NOT EXISTS labs')
        # CREATE TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS labs.weather_data (
                id SERIAL PRIMARY KEY,
                temp FLOAT,
                pressure FLOAT,
                humidity FLOAT,
                city VARCHAR(255),
                timestamp TIMESTAMP
            )
        ''')
        # INSERT DATA INTO TABLE
        cursor.execute('''
            INSERT INTO labs.weather_data (temp, pressure, humidity, city, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        ''', (transform_data['temp'], transform_data['pressure'], transform_data['humidity'], transform_data['city'], transform_data['timestamp']))
        conn.commit()
        conn.close()
    
    # Task 
    load_weather_data_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
    )

    # dependencies
    check_api_task >> fetch_data_task >> transform_data_task >> load_weather_data_task