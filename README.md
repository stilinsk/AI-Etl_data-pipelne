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




