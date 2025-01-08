import requests
import pandas as pd
from datetime import datetime

def get_weather_data(api_key, city_id):
    """
    Collect weather data from OpenWeatherMap API and return it as a DataFrame.

    Parameters:
    api_key (str): Your OpenWeatherMap API key
    city_id (str): City ID from OpenWeatherMap
    
    Returns:
    DataFrame: Weather data with timestamps, temperature, humidity, wind speed, and additional details.
    """
    base_url = "http://api.openweathermap.org/data/2.5/forecast"
    params = {
        'id': city_id,
        'appid': api_key,
        'units': 'metric'  # Metric units for temperature in Celsius
    }
    
    try:
        # Make the API request
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        weather_data = response.json()
        
        # Extract relevant data
        timestamps, temperatures, humidity, wind_speed = [], [], [], []
        pressure, visibility, rainfall, cloud_cover, icons, descriptions = [], [], [], [], [], []
        
        for item in weather_data['list']:
            timestamps.append(item['dt_txt'])
            temperatures.append(item['main']['temp'])
            humidity.append(item['main']['humidity'])
            wind_speed.append(item['wind']['speed'])
            pressure.append(item['main']['pressure'])
            visibility.append(item.get('visibility', None))
            rainfall.append(item.get('rain', {}).get('3h', 0))
            cloud_cover.append(item['clouds']['all'])
            icons.append(item['weather'][0]['icon'])
            descriptions.append(item['weather'][0]['description'])
        
        # Create a DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            'temperature': temperatures,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'pressure': pressure,
            'visibility': visibility,
            'rainfall': rainfall,
            'cloud_cover': cloud_cover,
            'icon': icons,
            'description': descriptions
        })
        
        # Convert timestamp column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def save_weather_data(df, filename='india_weather_data.csv'):
    """
    Save weather data to a CSV file.

    Parameters:
    df (DataFrame): The DataFrame to save
    filename (str): The file name for the CSV file (default is 'india_weather_data.csv')
    """
    try:
        # Append to existing file if it exists
        try:
            existing_df = pd.read_csv(filename)
            df = pd.concat([existing_df, df]).drop_duplicates(subset=['timestamp', 'city'])
        except FileNotFoundError:
            pass  # No existing file, proceed with saving new data
        
        df.to_csv(filename, index=False)
        print(f"Weather data saved to {filename}")
    except Exception as e:
        print(f"Error saving data: {e}")

if __name__ == "__main__":
    # Replace with your OpenWeatherMap API key
    API_KEY = "c9c53e4ef615028271edf6d041744a9b"
    
    # Dictionary of city IDs for Indian state capitals
    CITIES = {
        'Dehradun': "1273313",
        'Delhi': "1273294",
        'Mumbai': "1275339",
        'Kolkata': "1275004",
        'Chennai': "1264527",
        'Bengaluru': "1277333",
        'Hyderabad': "1269843",
        'Ahmedabad': "1279233",
        'Pune': "1259229",
        'Jaipur': "1269515",
        'Lucknow': "1264733",
        'Patna': "1260086",
        'Bhopal': "1275841",
        'Thiruvananthapuram': "1269743",
        'Guwahati': "1271476",
        'Ranchi': "1258526",
        'Shillong': "1256523",
        'Aizawl': "1277765",
        'Imphal': "1269750",
        'Itanagar': "1278341",
        'Agartala': "1278314",
        'Panaji': "1271157",
        'Raipur': "1273066",
        'Chandigarh': "1274746",
        'Srinagar': "1255634",
        'Shimla': "1256237",
        'Gangtok': "1254029",
    }
    
    all_states_weather_data = []

    print("Fetching weather data for all Indian states...")
    
    for city, city_id in CITIES.items():
        print(f"Fetching weather data for {city}...")
        weather_df = get_weather_data(API_KEY, city_id)
        
        if weather_df is not None:
            weather_df['city'] = city  # Add a city column to distinguish data
            all_states_weather_data.append(weather_df)
        else:
            print(f"Failed to fetch weather data for {city}.")
    
    # Combine all city data into a single DataFrame
    if all_states_weather_data:
        combined_df = pd.concat(all_states_weather_data, ignore_index=True)
        save_weather_data(combined_df, filename='india_weather_data.csv')
    else:
        print("No weather data fetched for any state.")
