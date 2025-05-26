from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests

CITIES = [
    'Nairobi', 'London', 'Kampala', 'Thika', 'Beijing',
    'New York', 'Paris', 'Tokyo', 'Sydney', 'Moscow',
    'Berlin', 'Madrid', 'Rome', 'Los Angeles', 'Chicago',
    'Toronto', 'Vancouver', 'Dubai', 'Singapore', 'Hong Kong',
    'Bangkok', 'Istanbul', 'Cairo', 'Johannesburg', 'Buenos Aires',
    'Lagos', 'Lima', 'Mumbai', 'Delhi', 'Shanghai',
    'Seoul', 'Mexico City', 'Jakarta', 'Rio de Janeiro', 'Sao Paulo',
    'Karachi', 'Manila', 'Tehran', 'Baghdad', 'Dhaka',
    'Kinshasa', 'Casablanca', 'Algiers', 'Accra'
]
POSTGRES_CONN_ID = 'postgres_default'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

def load_api_key():
    with open('/usr/local/airflow/dags/credential.txt', 'r') as file:
        return file.read().strip()

API_KEY = load_api_key()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        all_data = []
        for city in CITIES:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'
            }
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                all_data.append((city, data))
            else:
                raise Exception(f"Failed to fetch data for {city}. Status code: {response.status_code}")
        return all_data

    @task()
    def transform_weather_data(city_data_list):
        transformed_data = []
        for city, data in city_data_list:
            main = data['main']
            wind = data['wind']
            sys = data['sys']
            weather_code = data['weather'][0]['id']
            now = datetime.now()

            transformed_data.append({
                'city': city,
                'temperature': main['temp'],
                'feelsLike': main['feels_like'],
                'minimumTemp': main['temp_min'],
                'maximumTemp': main['temp_max'],
                'pressure': main['pressure'],
                'humidity': main['humidity'],
                'windspeed': wind['speed'],
                'winddirection': wind.get('deg', 0),
                'weathercode': weather_code,
                'timeRecorded': now,
                'sunrise': datetime.fromtimestamp(sys['sunrise']),
                'sunset': datetime.fromtimestamp(sys['sunset'])
            })
        return transformed_data

    @task()
    def load_weather_data(data_list):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table with UNIQUE constraint on city
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(255) PRIMARY KEY,
            temperature FLOAT,
            feelsLike FLOAT,
            minimumTemp FLOAT,
            maximumTemp FLOAT,
            pressure INT,
            humidity INT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timeRecorded TIMESTAMP,
            sunrise TIMESTAMP,
            sunset TIMESTAMP
        );
        """)

        for data in data_list:
            cursor.execute("""
            INSERT INTO weather_data (
                city, temperature, feelsLike, minimumTemp, maximumTemp,
                pressure, humidity, windspeed, winddirection, weathercode,
                timeRecorded, sunrise, sunset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                feelsLike = EXCLUDED.feelsLike,
                minimumTemp = EXCLUDED.minimumTemp,
                maximumTemp = EXCLUDED.maximumTemp,
                pressure = EXCLUDED.pressure,
                humidity = EXCLUDED.humidity,
                windspeed = EXCLUDED.windspeed,
                winddirection = EXCLUDED.winddirection,
                weathercode = EXCLUDED.weathercode,
                timeRecorded = EXCLUDED.timeRecorded,
                sunrise = EXCLUDED.sunrise,
                sunset = EXCLUDED.sunset;
            """, (
                data['city'], data['temperature'], data['feelsLike'],
                data['minimumTemp'], data['maximumTemp'], data['pressure'],
                data['humidity'], data['windspeed'], data['winddirection'],
                data['weathercode'], data['timeRecorded'], data['sunrise'],
                data['sunset']
            ))

        conn.commit()
        cursor.close()
        conn.close()

    extracted = extract_weather_data()
    transformed = transform_weather_data(extracted)
    load_weather_data(transformed)
