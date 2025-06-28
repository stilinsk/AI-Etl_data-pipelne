# AI-Etl_data-pipelne

[Watch this video to set up the first part of the video ](https://www.youtube.com/watch?v=J4_9GWLgPQY&t=1s)

In the following project we  will have 2 parts part one we will be getting the data from weather api and transform the data and load the data into postgres that is running on docker 



![ChatGPT Image Jun 21, 2025, 03_21_57 PM](https://github.com/user-attachments/assets/f6ccebde-eb31-47cc-99d3-ae44cb9cbf5a)

We will start by creating the environment of this project  

```
windows
cd Documents

mkdir phoenix

# Step 1: Create the environment
python -m venv etl

# Step 2: Activate the environment
etl\Scripts\activate
```
```
Linux /mac

cd ~/Documents    # Go to Documents folder in your home directory
mkdir phoenix     # Create a new folder called phoenix

# Step 1: Create the environment
python3 -m venv etl

# Step 2: Activate the environment
source etl/bin/activate

```

open vscode  
```
code .
```
we will create a file to see if we can acces our data 

```
import requests

API_KEY = '18890a1d8cfd1b3130af9ac578d2c1a5'

BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

CITIES = ['Nairobi', 'London', 'Tokyo', 'Delhi', 'New York']

for city in CITIES:
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        print(f"{city}: ✅ 200 OK")
    else:
        print(f"{city}: ❌ Failed - Status Code {response.status_code}")
```
📄 The Script, Explained Line by Line:
```
import requests
```
🔹 This tells Python:
"I want to use the requests library, which helps me talk to websites and APIs."

Before running this, you must install it once by typing:

```
pip install requests
```
```
API_KEY = 'your_api_key_here'
````
🔹 This is where you paste your API key from OpenWeather — a secret code that proves you have permission to use their data.
📌 Replace 'your_api_key_here' with the actual key they give you.

```
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
```
🔹 This is the main web address (URL) we’ll use to ask for weather data.

```
CITIES = ['Nairobi', 'London', 'Tokyo', 'Delhi', 'New York']
```
🔹 Here, we’re creating a list (like a shopping list) of 5 cities we want to get weather data for.

```
for city in CITIES:
```
🔹 This is a loop — it means:
“Go through each city in the list, one at a time, and do something with it.”

```
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
```
🔹 This creates a dictionary (a set of key-value pairs) with the information the API needs:

'q': city → which city are we asking about?

'appid': API_KEY → our secret key.

'units': 'metric' → this tells it to give temperature in Celsius (not Fahrenheit).

```
    response = requests.get(BASE_URL, params=params)
```
🔹 This sends the actual request to the weather website with all the info above, and saves the reply in a variable called response.

```
    if response.status_code == 200:
```
🔹 This checks:
“If the response came back with a 200 status code, it means everything went well ✅.”

```
        print(f"{city}: ✅ 200 OK")
```
🔹 If the API worked, we print a success message for that city.

```
    else:
        print(f"{city}: ❌ Failed - Status Code {response.status_code}")
```
🔹 If something went wrong (like a wrong API key or bad internet), we print an error with the code (like 401, 404, etc.)



| Code | Meaning             | What it Means for Us                     |
| ---- | ------------------- | ---------------------------------------- |
| 200  | ✅ OK                | Everything worked fine. We got the data. |
| 401  | ❌ Unauthorized      | API key is missing or incorrect.         |
| 404  | ❌ Not Found         | The city name is wrong or not found.     |
| 500  | ❌ Server Error      | Something went wrong on their side.      |
| 429  | ❌ Too Many Requests | You called the API too many times.       |



we can change the python code to get data printed in out terminal we create a file called data.py 
first see the structure of the json file 
```
https://api.openweathermap.org/data/2.5/weather?q=Nairobi&appid=18890a1d8cfd1b3130af9ac578d2c1a5
```

```
import requests

API_KEY = '18890a1d8cfd1b3130af9ac578d2c1a5'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

CITIES = ['Nairobi', 'London', 'Tokyo', 'Delhi', 'New York']

for city in CITIES:
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        temp = data['main']['temp']
        weather = data['weather'][0]['description']
        humidity = data['main']['humidity']
        wind_speed = data['feels']['like']
        
        print(f"\nWeather in {city}")
        print(f"-------------------")
        print(f"Temperature: {temp}°C")
        print(f"Condition: {weather}")
        print(f"Humidity: {humidity}%")
        print(f"Wind Speed: {wind_speed} m/s")
    else:
        print(f"{city}: ❌ Failed - Status Code {response.status_code}")
```
Next we will be getting our data and transforming it to a csv and load it to a csv file 

```
# Import necessary Python modules
import json                  # To work with JSON data from the API
import csv                   # To write data into a CSV file
from datetime import datetime  # To handle date and time formatting
import requests              # To make HTTP requests to the weather API

# List of cities to collect weather data for
# You can add or remove cities as needed

city_names = [
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


# This is the base URL for OpenWeather API.
# We'll add the city name and API key to it later.

base_url = 'https://api.openweathermap.org/data/2.5/weather?q='

# Read your API key from a local file.
# Make sure the file "credentials.txt" exists and contains only the API key.

with open("credentials.txt", 'r') as f:
    api_key = f.read().strip()  # Removes any extra whitespace or new lines

# Function to convert temperature from Kelvin (API default) to Celsius.

def kelvin_to_celsius(temp_k):
    return temp_k - 273.15


# Function to fetch and transform weather data for one city.
# Returns a dictionary with clean and structured data if successful,
# otherwise prints the error and returns None.

def etl_weather_data(city):
    # Build the complete API URL by adding city and API key
    url = base_url + city + "&APPID=" + api_key

    # Send a GET request to the weather API
    response = requests.get(url)

    # Convert the response to a dictionary
    data = response.json()

    # If the request was successful (HTTP status code 200)
    if response.status_code == 200:
        return {
            "city": data["name"],  # City name
            "description": data["weather"][0]['description'],  # Weather condition
            "temperature": kelvin_to_celsius(data["main"]["temp"]),  # Current temperature
            "feelsLike": kelvin_to_celsius(data["main"]["feels_like"]),  # Feels-like temperature
            "minimumTemp": kelvin_to_celsius(data["main"]["temp_min"]),  # Minimum temperature
            "maximumTemp": kelvin_to_celsius(data["main"]["temp_max"]),  # Maximum temperature
            "pressure": data["main"]["pressure"],  # Atmospheric pressure
            "humidity": data["main"]["humidity"],  # Humidity %
            "windSpeed": data["wind"]["speed"],  # Wind speed in m/s
            "timeRecorded": datetime.utcfromtimestamp(data['dt'] + data['timezone']).isoformat(),  # Timestamp
            "sunrise": datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']).isoformat(),  # Sunrise time
            "sunset": datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']).isoformat()  # Sunset time
        }
    else:
        # If API call fails, print the error message
        print(f"Failed to fetch data for {city}. Error: {data.get('message')}")
        return None


# Main function that coordinates the workflow
# - Creates the CSV file
# - Fetches weather for each city
# - Writes each result into the CSV

def main():
    csv_file = "weather_data.csv"  # Output file name

    # Open CSV file once and write the header row
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "city", "description", "temperature", "feelsLike", "minimumTemp",
            "maximumTemp", "pressure", "humidity", "windSpeed",
            "timeRecorded", "sunrise", "sunset"
        ])
        writer.writeheader()  # Write column names at the top of the file

        # Loop through all the cities one by one
        for city in city_names:
            print(f"Fetching data for {city}...")  # Let user know the process has started
            weather_data = etl_weather_data(city)  # Get the weather data

            # If data was fetched successfully, write it to the file
            if weather_data:
                writer.writerow(weather_data)  # Add the row to CSV
                print(f"✔️ Saved weather for {city}")
            else:
                print(f"❌ Could not fetch data for {city}")


# This block ensures that main() runs only
# when the script is run directly (not imported as a module)

if __name__ == "__main__":
    main()
```
we will get docker up and running now so that we can get our sink (postgres) ready .


we will create a file called docker-compose.yml
```
version: '3'
services:
  postgres:
    image: postgres:13
    container_name: postgres_db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./dags:/usr/local/airflow/dags
      - ./dags/credential.txt:/usr/local/airflow/dags/credential.txt
volumes:
  postgres_data:
```
We will look if our postgres port is open and we will  open commad prompt as administrator and run 

```
netstat -aon | findstr :5432
 if we get output like  TCP    127.0.0.1:5432       ...       PID:1234
 then run  taskkill /PID 1234 /F
```
##### How docker works under the hood 

Imagine your application (like Airflow) is a ship. Normally, when you move this ship from one computer (your laptop) to another (a server or cloud), it might break because:

The new machine has different settings or tools installed

Some dependencies are missing

Different operating systems behave differently

Docker is like putting your ship into a shipping container.
It packages your app with everything it needs to run — like its operating system, libraries, settings, etc.

 So no matter where you "ship" it, it runs exactly the same.

 we will need to install some few things 
 ```
pip install --upgrade apache-airflow  // same thing as    pip install apache-airflow

pip install apache-airflow-providers-postgres
```
we will create or airflow dag and  add it to dag folder 
```
# Import core Airflow components
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook  # To connect to PostgreSQL
from airflow.decorators import task  # For declaring Python functions as tasks

# Import standard libraries
from datetime import datetime, timedelta
import requests  # To make HTTP requests to external APIs

# List of cities to collect weather data for
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

# The Airflow connection ID for PostgreSQL (configured in the Airflow UI or environment)
POSTGRES_CONN_ID = 'postgres_default'

# The base URL for the OpenWeatherMap API
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

# Function to read the API key from a file
def load_api_key():
    with open('/usr/local/airflow/dags/credentials.txt', 'r') as file:
        return file.read().strip()

API_KEY = load_api_key()

# Default parameters for the DAG (like retry behavior, owner, etc.)
default_args = {
    'owner': 'airflow',  # The owner of the DAG
    'start_date': datetime(2025, 6, 21),  # When the DAG should start running
    'retries': 3,  # Number of times to retry on failure
    'retry_delay': timedelta(minutes=5)  # Wait time between retries
}

# Define the DAG
with DAG(
    dag_id='weather_etl_pipeline',  # Unique name for the DAG
    default_args=default_args,
    schedule='@daily',  # Run once per day
    catchup=False  # Do not backfill for previous dates
) as dag:

    # Task: Extract weather data for each city
    @task()
    def extract_weather_data():
        all_data = []
        for city in CITIES:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'  # Celsius
            }
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                all_data.append((city, data))
            else:
                # If the API call fails, raise an error to fail the task
                raise Exception(f"Failed to fetch data for {city}. Status code: {response.status_code}")
        return all_data  # Returns a list of (city, data) tuples

    # Task: Clean and format the weather data
    @task()
    def transform_weather_data(city_data_list):
        transformed_data = []
        for city, data in city_data_list:
            main = data['main']
            wind = data['wind']
            sys = data['sys']
            weather_code = data['weather'][0]['id']
            now = datetime.now()

            # Create a clean Python dictionary for each city’s weather
            transformed_data.append({
                'city': city,
                'temperature': main['temp'],
                'feelsLike': main['feels_like'],
                'minimumTemp': main['temp_min'],
                'maximumTemp': main['temp_max'],
                'pressure': main['pressure'],
                'humidity': main['humidity'],
                'windspeed': wind['speed'],
                'winddirection': wind.get('deg', 0),  # Default to 0 if missing
                'weathercode': weather_code,
                'timeRecorded': now,
                'sunrise': datetime.fromtimestamp(sys['sunrise']),
                'sunset': datetime.fromtimestamp(sys['sunset'])
            })
        return transformed_data

    # Task: Load data into PostgreSQL using PostgresHook
    @task()
    def load_weather_data(data_list):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create a table if it doesn’t exist
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

        # Insert or update records using UPSERT (Postgres ON CONFLICT)
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

        # Finalize the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()

    # Set the order of execution: extract → transform → load
    extracted = extract_weather_data()
    transformed = transform_weather_data(extracted)
    load_weather_data(transformed)
```

 
 Then we will go ahead and install astro 
  
###### Windows
   ```
winget install -e --id Astronomer.Astro

astro dev init

astro dev start --verbosity debug

```
brew install astro

astro dev init

astro dev start --verbosity debug
```
  
###### MAC/ LINUX 
```
By now we ahould be  able to see our dag in our UI at localhost 8080


![airflow](https://github.com/user-attachments/assets/9777990b-8bb1-4813-a544-c06a25b76411)

we will proceed to make the following connections before running our dag 

open connections and add new connections 

connection id - postgres default  from our dag 

connection type - postgres 

host go to docker container  and open postgres image  and copy the name on top 

add schema as postgres - or databse name and save and trigger the dag

open the  docker image under exec and run   
```
psql -U postgres -d postgres
```
   psq used to connect to postgres U means user and d means database 


brings us to end of part 1



PART 2 
![ChatGPT Image Jun 22, 2025, 05_16_29 PM](https://github.com/user-attachments/assets/ac39ac19-5ccc-4ab2-b588-94466c661ccf)


 we will create n AI AGENT  to connect to our project 

 we will first create an .env file to store our  api key and connection to our databse 
 ```
OPENAI_API_KEY=
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=postgres
PG_DATABASE=postgres
````
```
# Import necessary libraries for environment variables, web app, database, AI, and utilities
from dotenv import load_dotenv  # Loads environment variables from a .env file
load_dotenv()  # Loads the .env file into the script's environment

import streamlit as st  # Streamlit for creating interactive web apps
import os  # For accessing environment variables
import psycopg2  # For connecting to and querying PostgreSQL databases
from openai import OpenAI  # For interacting with OpenAI's API
import traceback  # For handling and displaying error stack traces
from typing import List, Dict, Tuple, Optional  # Type hints for better code clarity
from datetime import datetime  # For handling timestamps
import json  # For parsing and formatting JSON data

# Initialize Streamlit session state to persist data across reruns
if 'query_history' not in st.session_state:
    st.session_state.query_history = []  # List to store past queries and their details
if 'current_query' not in st.session_state:
    st.session_state.current_query = None  # Index of the currently selected query
if 'follow_up_active' not in st.session_state:
    st.session_state.follow_up_active = False  # Flag for follow-up question processing
if 'follow_up_question' not in st.session_state:
    st.session_state.follow_up_question = ""  # Text of the follow-up question

# Initialize OpenAI client with API key from environment variables
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Define the database schema for the weather_data table
SCHEMA_KNOWLEDGE = {
    "weather_data": {
        "columns": {
            "city": "VARCHAR - Name of the city",
            "temperature": "FLOAT - Temperature in Celsius",
            "windspeed": "FLOAT - Wind speed in km/h",
            "winddirection": "FLOAT - Wind direction in degrees",
            "weathercode": "INT - Code representing weather condition",
            "timerecorded": "TIMESTAMP - When the data was recorded",
            "humidity": "INT - Humidity percentage",
            "pressure": "INT - Atmospheric pressure in hPa",
        },
        "sample_queries": [
            "SELECT city, temperature FROM weather_data WHERE temperature > 30 ORDER BY temperature DESC;",
            "SELECT city, AVG(temperature) as avg_temp FROM weather_data GROUP BY city HAVING AVG(temperature) > 20;",
            "SELECT city, COUNT(*) as records_count FROM weather_data GROUP BY city ORDER BY records_count DESC;"
        ]
    }
}

# Define geographic knowledge mapping continents to cities
GEOGRAPHIC_KNOWLEDGE = {
    "africa": [
        'Nairobi', 'Kampala', 'Thika', 'Cairo', 'Johannesburg',
        'Lagos', 'Casablanca', 'Algiers', 'Accra', 'Kinshasa',
        'Addis Ababa', 'Dar es Salaam', 'Abidjan', 'Alexandria', 'Khartoum'
    ],
    "europe": [
        'London', 'Paris', 'Moscow', 'Berlin', 'Madrid',
        'Rome', 'Istanbul', 'Lisbon', 'Vienna', 'Prague'
    ],
    "asia": [
        'Beijing', 'Tokyo', 'Dubai', 'Singapore', 'Hong Kong',
        'Bangkok', 'Mumbai', 'Delhi', 'Shanghai', 'Seoul'
    ],
    "north_america": [
        'New York', 'Los Angeles', 'Chicago', 'Toronto', 'Vancouver',
        'Mexico City', 'Houston', 'Montreal', 'San Francisco', 'Miami'
    ],
    "south_america": [
        'Buenos Aires', 'Lima', 'Rio de Janeiro', 'Sao Paulo',
        'Bogota', 'Santiago', 'Caracas', 'Quito', 'Montevideo'
    ],
    "australia": [
        'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide'
    ]
}

# Define SQLAgent class to handle query processing and generation
class SQLAgent:
    def __init__(self):
        # Initialize instance variables for tracking thought process and memory
        self.thinking_steps = []  # List to log thought process steps
        self.self_correction_attempts = 0  # Counter for SQL correction attempts
        self.max_self_correction_attempts = 3  # Maximum allowed correction attempts
        self.context_memory = []  # List to store query history for context

    def log_thinking(self, step: str, content: str):
        """Record the agent's thought process with timestamp"""
        # Append a new step to thinking_steps with timestamp, step name, and content
        self.thinking_steps.append({
            "timestamp": datetime.now().isoformat(),
            "step": step,
            "content": content
        })

    def add_to_memory(self, question: str, sql: str, results: List[Dict]):
        """Add query details to context memory for future reference"""
        # Store question, SQL, and a sample of results in context_memory
        self.context_memory.append({
            "question": question,
            "sql": sql,
            "results_summary": str(results[:3]) if results else "No results",
            "timestamp": datetime.now().isoformat()
        })

    def get_context_summary(self) -> str:
        """Generate a summary of previous queries for context"""
        # Return a formatted string of past queries or a default message if none exist
        if not self.context_memory:
            return "No previous queries in this session."
        summary = "Previous queries in this session:\n"
        for i, item in enumerate(self.context_memory, 1):
            summary += f"\n{i}. Question: {item['question']}\n   SQL: {item['sql']}\n   Results (sample): {item['results_summary']}\n"
        return summary

    def analyze_question(self, question: str) -> Dict:
        """Analyze the user's question using OpenAI to extract intent and entities"""
        # Get context from previous queries
        context = self.get_context_summary()
        # Create a prompt for OpenAI to analyze the question
        analysis_prompt = f"""
        Analyze this weather data question deeply, considering this context:
        {context}
        
        Current question: {question}
        
        Detect if the question mentions any continents (Africa, Asia, Europe, etc.) or specific cities.
        
        Respond with a valid JSON object containing these fields:
        - intent: What the user wants to know
        - entities: List of important entities
        - timeframe: Any mentioned timeframe or null
        - comparisons: List of any comparisons
        - aggregations: List of any aggregation needs
        - language: Detected language
        - references_previous: Whether this references previous queries (true/false)
        - reference_details: How this relates to previous queries if applicable
        - geographic_scope: The continent mentioned if any (e.g., "Africa", "Asia")
        - mentioned_cities: List of specifically mentioned cities
        """
        # Call OpenAI API to analyze the question
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a sophisticated question analyzer with context awareness."},
                {"role": "user", "content": analysis_prompt}
            ]
        )
        try:
            # Parse the JSON response from OpenAI
            analysis = json.loads(response.choices[0].message.content)
            self.log_thinking("Question Analysis", json.dumps(analysis, indent=2))
            return analysis
        except json.JSONDecodeError:
            # Return a default analysis if JSON parsing fails
            return {
                "intent": "unknown",
                "entities": [],
                "timeframe": None,
                "comparisons": [],
                "aggregations": [],
                "language": "english",
                "references_previous": False,
                "reference_details": "",
                "geographic_scope": None,
                "mentioned_cities": []
            }

    def generate_sql_plan(self, analysis: Dict, question: str) -> str:
        """Create a step-by-step plan for SQL generation"""
        # Get context from previous queries
        context = self.get_context_summary()
        # Create a prompt for OpenAI to generate a SQL plan
        plan_prompt = f"""
        Based on this analysis:
        {json.dumps(analysis, indent=2)}
        
        Previous queries context:
        {context}
        
        And this database schema:
        {SCHEMA_KNOWLEDGE}
        
        Available continents and their cities:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Create a step-by-step plan to generate the SQL query. Consider:
        1. Which tables and columns are needed
        2. What filters/conditions to apply (including geographic filters if needed)
        3. Any sorting/grouping requirements
        4. How to handle the specific intent
        5. How this relates to previous queries if relevant
        6. How to handle any geographic scope mentioned
        
        Respond with clear numbered steps.
        """
        # Call OpenAI API to generate the plan
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL query planner with context awareness."},
                {"role": "user", "content": plan_prompt}
            ]
        )
        # Extract and log the plan
        plan = response.choices[0].message.content
        self.log_thinking("SQL Generation Plan", plan)
        return plan

    def generate_initial_sql(self, plan: str, question: str, analysis: Dict) -> str:
        """Generate initial SQL query based on the plan"""
        # Get context if the question references previous queries
        context = self.get_context_summary() if analysis.get("references_previous", False) else ""
        # Check if a geographic scope is mentioned
        if analysis.get("geographic_scope"):
            continent = analysis["geographic_scope"].lower()
            if continent in GEOGRAPHIC_KNOWLEDGE:
                cities = GEOGRAPHIC_KNOWLEDGE[continent]
                city_list = ", ".join([f"'{city}'" for city in cities])
                # Handle average temperature queries for a continent
                if "average" in question.lower() or "avg" in question.lower():
                    sql_prompt = f"""
                    Based on this plan:
                    {plan}
                    
                    Previous queries context (if relevant):
                    {context}
                    
                    And the original question:
                    {question}
                    
                    Generate a PostgreSQL SQL query that calculates the average temperature 
                    for all cities in {continent} as a single value.
                    
                    The query should:
                    1. Filter for cities in {continent} only
                    2. Calculate the average temperature across all these cities
                    3. Not include city in the SELECT or GROUP BY since we want one aggregate value
                    
                    Example correct query:
                    SELECT AVG(temperature) as avg_temp FROM weather_data WHERE city IN ('Nairobi', 'Cairo', ...);
                    
                    Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
                    """
                else:
                    # Handle general queries with geographic filter
                    sql_prompt = f"""
                    Based on this plan:
                    {plan}
                    
                    Previous queries context (if relevant):
                    {context}
                    
                    And the original question:
                    {question}
                    
                    Generate a PostgreSQL SQL query for the weather_data table.
                    Apply this geographic filter: WHERE city IN ({city_list})
                    Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
                    """
        else:
            # Handle queries without geographic scope
            sql_prompt = f"""
            Based on this plan:
            {plan}
            
            Previous queries context (if relevant):
            {context}
            
            And the original question:
            {question}
            
            Generate a PostgreSQL SQL query for the weather_data table.
            Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
            """
        # Call OpenAI API to generate the SQL
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a PostgreSQL expert. Generate SQL only - no markdown, no code blocks, just pure SQL."},
                {"role": "user", "content": sql_prompt}
            ]
        )
        # Clean and log the generated SQL
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        self.log_thinking("Initial SQL Generation", sql)
        return sql

    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """Validate SQL query for syntax and logical correctness"""
        # Check for markdown syntax in the query
        if "```" in sql:
            return False, "Query contains markdown code block syntax"
        # Create a prompt for OpenAI to validate the SQL
        validation_prompt = f"""
        Validate this PostgreSQL SQL query for syntax and logical correctness:
        {sql}
        
        The database has this schema:
        {SCHEMA_KNOWLEDGE}
        
        Available geographic knowledge:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Respond in this exact format:
        VALID: true|false
        REASON: "reason for validation result"
        """
        # Call OpenAI API to validate
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL validator checking for syntax errors and logical consistency with the schema."},
                {"role": "user", "content": validation_prompt}
            ]
        )
        # Parse the validation result
        result = response.choices[0].message.content
        valid = "VALID: true" in result
        reason = result.split("REASON: ")[1].strip('"') if "REASON: " in result else "Unknown error"
        self.log_thinking("SQL Validation", f"Valid: {valid}\nReason: {reason}")
        return valid, reason

    def self_correct_sql(self, sql: str, validation_result: str, question: str) -> Optional[str]:
        """Attempt to correct an invalid SQL query"""
        # Check if maximum correction attempts are reached
        if self.self_correction_attempts >= self.max_self_correction_attempts:
            return None
        # Create a prompt for OpenAI to correct the SQL
        correction_prompt = f"""
        The following SQL query was invalid:
        {sql}
        
        Validation error:
        {validation_result}
        
        Original question:
        {question}
        
        Database schema:
        {SCHEMA_KNOWLEDGE}
        
        Geographic knowledge:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Please correct the SQL query. Return ONLY the corrected SQL with no additional text.
        """
        # Call OpenAI API to correct the SQL
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL corrector."},
                {"role": "user", "content": correction_prompt}
            ]
        )
        # Increment correction attempts and log the corrected SQL
        corrected_sql = response.choices[0].message.content.strip()
        self.self_correction_attempts += 1
        self.log_thinking(f"Self-Correction Attempt {self.self_correction_attempts}", corrected_sql)
        return corrected_sql

    def explain_query(self, sql: str, question: str, language: str, analysis: Dict) -> str:
        """Generate a detailed explanation of the SQL query"""
        # Get context if the question references previous queries
        context = self.get_context_summary() if analysis.get("references_previous", False) else ""
        # Create a prompt for OpenAI to explain the SQL
        explanation_prompt = f"""
        The user asked (in {language}): "{question}"
        
        Context from previous queries:
        {context}
        
        The generated SQL query is:
        {sql}
        
        Database schema:
        {SCHEMA_KNOWLEDGE}
        
        Geographic context:
        {f"Continent: {analysis['geographic_scope']}" if analysis.get('geographic_scope') else ""}
        {f"Mentioned cities: {analysis['mentioned_cities']}" if analysis.get('mentioned_cities') else ""}
        
        Provide a detailed explanation in {language.capitalize()} covering:
        1. The technical approach taken
        2. Why this query answers the question
        3. Any important considerations or limitations
        4. How this relates to previous queries if relevant
        5. Alternative approaches that could have been taken
        
        Structure the explanation clearly with appropriate headings.
        """
        # Call OpenAI API to generate the explanation
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"You explain SQL queries in {language} with technical depth and context awareness."},
                {"role": "user", "content": explanation_prompt}
            ]
        )
        return response.choices[0].message.content

    def generate_visualization_recommendation(self, sql: str, results: List[Dict]) -> str:
        """Recommend a visualization for the query results"""
        # Create a prompt for OpenAI to recommend a visualization
        recommendation_prompt = f"""
        Based on this SQL query:
        {sql}
        
        And these sample results (first 3 rows):
        {results[:3]}
        
        Recommend the best way to visualize this data. Consider:
        - Chart type (bar, line, pie, etc.)
        - What to put on each axis
        - Any important annotations
        - Color recommendations
        - Geographic aspects if relevant
        
        Provide a detailed recommendation with reasoning.
        """
        # Call OpenAI API to generate the recommendation
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a data visualization expert."},
                {"role": "user", "content": recommendation_prompt}
            ]
        )
        return response.choices[0].message.content

    def generate_follow_up_questions(self, question: str, sql: str, results: List[Dict]) -> List[str]:
        """Generate 3-5 relevant follow-up questions"""
        # Create a prompt for OpenAI to generate follow-up questions
        prompt = f"""
        Based on this interaction:
        User question: {question}
        SQL query: {sql}
        Results sample: {results[:3] if results else "No results"}
        
        Generate 3-5 relevant follow-up questions the user might ask next.
        Consider geographic context if relevant.
        Return as a JSON list like this:
        ["question1", "question2", "question3"]
        """
        # Call OpenAI API to generate follow-ups
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You generate insightful follow-up questions."},
                {"role": "user", "content": prompt}
            ]
        )
        try:
            # Parse the JSON response
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            # Return default follow-ups if parsing fails
            return [
                f"What's the average temperature for cities in these results?",
                f"Can you show me the wind speed distribution?",
                f"How does this compare to data from last month?"
            ]

# Define DatabaseManager class to handle database connections and queries
class DatabaseManager:
    def __init__(self):
        # Initialize connection parameters from environment variables
        self.conn_params = {
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "dbname": os.getenv("PG_DATABASE")
        }

    def execute_query(self, query: str) -> Tuple[List[str], List[Tuple]]:
        """Execute a SQL query and return column names and rows"""
        # Check for markdown syntax in the query
        if "```" in query:
            raise ValueError("Query contains markdown syntax and cannot be executed")
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**self.conn_params)
        try:
            with conn.cursor() as cur:
                cur.execute(query)  # Execute the SQL query
                if cur.description:  # Check if the query returned columns
                    colnames = [desc[0] for desc in cur.description]  # Get column names
                    rows = cur.fetchall()  # Fetch all rows
                    return colnames, rows
                return [], []  # Return empty lists if no data
        finally:
            conn.close()  # Ensure the connection is closed

# Function to display query results in the Streamlit app
def display_query_result(query_data):
    """Display query details, results, and recommendations"""
    # Show a header with the query's timestamp
    st.subheader(f"🔍 Query from {query_data['timestamp']}")
    # Create an expandable section for details
    with st.expander("View Details", expanded=True):
        st.markdown(f"**Question:** {query_data['question']}")  # Display the question
        st.markdown("**Generated SQL:**")  # Label for SQL
        st.code(query_data['sql'], language="sql")  # Display SQL with syntax highlighting
        st.markdown("**Explanation:**")  # Label for explanation
        st.markdown(query_data['explanation'])  # Display the explanation
        # Check if there are results to display
        if query_data['results']:
            st.markdown("**Results:**")  # Label for results
            st.dataframe(  # Display results in a table
                data=query_data['results'],
                use_container_width=True,  # Fit table to container
                hide_index=True,  # Hide row indices
                height=min(400, 35 * len(query_data['results']) + 3),  # Dynamic table height
            )
            st.markdown("**Visualization Recommendation:**")  # Label for visualization
            st.markdown(query_data['visualization_rec'])  # Display visualization recommendation
        else:
            st.info("No results found for this query.")  # Inform if no results
        # Display follow-up questions if available
        if query_data['follow_ups']:
            st.markdown("**Suggested Follow-ups:**")  # Label for follow-ups
            for i, question in enumerate(query_data['follow_ups'], 1):
                st.markdown(f"{i}. {question}")  # Display numbered follow-ups

# Main function to run the Streamlit app
def main():
    # Configure Streamlit app settings
    st.set_page_config(
        page_title="Advanced Weather Data AI Agent with Memory",
        page_icon="🌍",  # Globe icon for the app
        layout="wide"  # Wide layout for more space
    )
    # Display app title
    st.title("🌦️ Advanced Weather Data Query Agent with Memory")
    # Describe the agent's capabilities
    st.markdown("""
    This AI agent can:
    - Remember previous queries in your session
    - Understand follow-up questions in context
    - Provide continuous analysis without losing history
    - Suggest relevant follow-up questions
    """)
    # Initialize session state variables if not set
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'current_query' not in st.session_state:
        st.session_state.current_query = None
    if 'follow_up_active' not in st.session_state:
        st.session_state.follow_up_active = False
    if 'follow_up_context' not in st.session_state:
        st.session_state.follow_up_context = ""
    # Display query history in the sidebar
    if st.session_state.query_history:
        st.sidebar.title("Query History")
        for i, query in enumerate(st.session_state.query_history):
            # Create buttons for each query in history
            if st.sidebar.button(f"Query {i+1}: {query['question'][:50]}...", key=f"hist_{i}"):
                st.session_state.current_query = i
        # Display the selected query's details
        if st.session_state.current_query is not None:
            display_query_result(st.session_state.query_history[st.session_state.current_query])
    # Create a form for user input
    with st.form("main_query_form"):
        # Text area for the user's question
        question = st.text_area("Ask your weather data question (English or Swahili):", 
                              height=100,
                              key="main_question")
        # Submit button for the form
        submitted = st.form_submit_button("Analyze with AI Agent", type="primary")
        # Process the question if submitted
        if submitted and question:
            agent = SQLAgent()  # Create a new SQLAgent instance
            db_manager = DatabaseManager()  # Create a new DatabaseManager instance
            # Show processing status
            with st.status("Processing your question...", expanded=True) as status:
                try:
                    st.write("🔍 Analyzing your question with context...")  # Indicate analysis
                    analysis = agent.analyze_question(question)  # Analyze the question
                    language = analysis.get("language", "english")  # Get detected language
                    st.write("📝 Creating execution plan...")  # Indicate planning
                    plan = agent.generate_sql_plan(analysis, question)  # Generate SQL plan
                    st.write("💻 Generating SQL query...")  # Indicate SQL generation
                    sql = agent.generate_initial_sql(plan, question, analysis)  # Generate SQL
                    # Validate the SQL query
                    valid, reason = agent.validate_sql(sql)
                    while not valid and agent.self_correction_attempts < agent.max_self_correction_attempts:
                        st.warning(f"⚠️ Found issue: {reason}")  # Warn about validation issues
                        st.write("🔄 Attempting self-correction...")  # Indicate correction
                        sql = agent.self_correct_sql(sql, reason, question)  # Correct SQL
                        if sql:
                            valid, reason = agent.validate_sql(sql)  # Re-validate
                        else:
                            break
                    # Handle invalid SQL after max attempts
                    if not valid:
                        st.error("❌ Could not generate valid SQL after multiple attempts.")
                        st.code(reason, language="text")
                        status.update(label="Processing failed", state="error")
                        return
                    st.write("⚡ Executing query...")  # Indicate query execution
                    colnames, rows = db_manager.execute_query(sql)  # Execute the query
                    results = [dict(zip(colnames, row)) for row in rows] if colnames else []  # Format results
                    st.write("📖 Generating explanations...")  # Indicate explanation generation
                    explanation = agent.explain_query(sql, question, language, analysis)  # Generate explanation
                    # Generate visualization and follow-ups if results exist
                    if results:
                        visualization_rec = agent.generate_visualization_recommendation(sql, results)
                        follow_ups = agent.generate_follow_up_questions(question, sql, results)
                    else:
                        visualization_rec = "No results to visualize."
                        follow_ups = []
                    agent.add_to_memory(question, sql, results)  # Add to context memory
                    # Store query details
                    query_data = {
                        "question": question,
                        "sql": sql,
                        "explanation": explanation,
                        "results": results,
                        "visualization_rec": visualization_rec,
                        "follow_ups": follow_ups,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "thinking_steps": agent.thinking_steps
                    }
                    # Append to query history and set as current query
                    st.session_state.query_history.append(query_data)
                    st.session_state.current_query = len(st.session_state.query_history) - 1
                    status.update(label="Processing complete!", state="complete")  # Update status
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")  # Display error
                    st.code(traceback.format_exc(), language="python")  # Show traceback
                    status.update(label="Processing failed", state="error")  # Update status
                    return
            st.rerun()  # Rerun the app to refresh the UI

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()
```

