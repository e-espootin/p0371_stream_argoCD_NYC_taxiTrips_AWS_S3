from datetime import datetime
import os
import requests
from typing import Dict, Any
#from airflow.models import Variable


class OpenWeatherMapAPIClass:
    def __init__(self):
    
        # Access secrets from environment variables
        self.openweathermap_api_key = os.getenv("KEY")
        self.api_key = self.openweathermap_api_key#Variable.get('openweathermap_api_key')
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"

    def get_weather_data(self, city_name: str) -> Dict[str, Any]:
        params = {
            "q": city_name,
            "appid": self.api_key,
            "units": "metric"  # Use metric units for temperature in Celsius
        }
        
        response = requests.get(self.base_url, params=params)
        #response.raise_for_status()  # Raise an exception for HTTP errors
        
        return response.json()

    def get_formatted_weather_data(self, city_name: str, project_id:int) -> Dict[str, Any]:
        data = self.get_weather_data(city_name)
    

        return {
            "project_id": project_id,
            "city": data["name"],
            "country": data["sys"]["country"],
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "main": data["weather"][0]["main"],
            "description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "created_timestamp": datetime.now().isoformat()
     

        }
    
    
