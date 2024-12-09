
import random
from faker import Faker
import pandas as pd
from ctgan import CTGAN
# from scripts import openWeatherMapApi

class DataGenerator:
    def __init__(self, example_file="sample_data/taxi_tripdata_top1000.csv"):
        self.example_file = example_file
        self.df = pd.DataFrame()
        


    # validate , if inputed file exists
    def validate_sample_file(self):
        try:
            data = pd.read_csv(self.example_file)
            # data = data.head(1000)
            # data.to_csv('sample_data/taxi_tripdata_top1000.csv', index=False)
            self.df = data
            return True
        except FileNotFoundError:
            print(f"File not found: {self.example_file}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
        
    
    # generate using Faker library
    def generate_sample_data_faker(self, num_samples=1):
        try:
            fake = Faker()
            # get one random row from the dataset
            sampled_rand_row = self.df.sample(n=1, random_state=42).to_dict(orient='records')[0]
            sample_data = []
            
            # call weather api
            # weather = openWeatherMapApi.OpenWeatherMapAPIClass()
            # ny_weather = weather.get_formatted_weather_data("New York", 1)


            for _ in range(num_samples):
                record = {
                    "VendorID": random.randint(1, 2),
                    "lpep_pickup_datetime": fake.date_time_this_decade().isoformat(),
                    "lpep_dropoff_datetime": '',
                    "request_datetime": '',
                    "passenger_count": random.randint(1, 4),
                    "trip_distance": sampled_rand_row["trip_distance"],
                    "RatecodeID": sampled_rand_row["RatecodeID"],
                    "store_and_fwd_flag": sampled_rand_row["store_and_fwd_flag"],
                    "PULocationID": sampled_rand_row["PULocationID"],
                    "DOLocationID": sampled_rand_row["DOLocationID"],
                    "payment_type": sampled_rand_row["payment_type"],
                    "fare_amount": sampled_rand_row["fare_amount"],
                    "extra": sampled_rand_row["extra"],
                    "mta_tax": sampled_rand_row["mta_tax"] if sampled_rand_row["mta_tax"] else random.uniform(0.5, 1.0),
                    "tip_amount": random.uniform(0, 20),
                    "tolls_amount": sampled_rand_row["tolls_amount"],
                    "improvement_surcharge": sampled_rand_row["improvement_surcharge"],
                    "total_amount": random.uniform(5, 100),
                    "congestion_surcharge": sampled_rand_row["congestion_surcharge"],
                    "airport_fee": random.uniform(5, 100),
                    # add some irrelevant sensitive data for test purposes - driver
                    "driver_email": fake.email(),
                    "driver_phone_number": fake.phone_number(),
                    "driver_fullname": fake.name() + " " + fake.last_name(),
                    "driver_credit_card": fake.credit_card_number(),
                    # add some irrelevant sensitive data for test purposes - passenger
                    "passenger_email": fake.email(),
                    "passenger_phone_number": fake.phone_number(),
                    "passenger_fullname": fake.name() + " " + fake.last_name(),
                    "passenger_credit_card": fake.credit_card_number(),
                    "passenger_address": str.replace(str.replace(fake.address(), ',', ' - '), "\n", ' '),
                    "passenger_Job": fake.job(),
                    "passenger_age": random.randint(18, 60),
                    "passenger_sex": random.choice(["M", "F"]),
                    # imaginery geolocation data in New York city
                    "pickup_latitude": random.uniform(40.5, 40.9),
                    "pickup_longitude": random.uniform(-74.5, -73.5),
                    "dropoff_latitude": random.uniform(40.5, 40.9),
                    "dropoff_longitude": random.uniform(-74.5, -73.5),
                    # Air pollution quality data for NY
                    "pickup_AQI": random.randint(20, 80),
                    "dropoff_AQI": random.randint(20, 80),
                    # Weather data for NY
                    "temperature": random.randint(-10, 45),
                    "humidity": random.randint(0, 100),
                    "pickup_precipitation_chance": random.uniform(0, 100),
                    "uv_index": random.randint(0, 100),
                    "feels_like": random.randint(0, 100),
                    "weather_description": random.choice(["Clear", "Cloudy", "Rain", "Snow", "Fog"]),
                    "wind_speed_km": random.randint(2, 35),
                  

                }
                record["lpep_dropoff_datetime"] = (pd.to_datetime(record["lpep_pickup_datetime"]) + pd.to_timedelta(random.randint(1, 60), unit='m')).isoformat()
                record["request_datetime"] = (pd.to_datetime(record["lpep_pickup_datetime"]) - pd.to_timedelta(random.randint(1, 120), unit='m')).isoformat()
                # print(f"record: {record}")
                sample_data.append(record)
            
            return sample_data
        except Exception as e:
            print(f"Error in generate_sample_data_faker() >> : {e}")
            return False
    
    # generate using GANs method
    def generate_sample_data_gans(self, num_samples=1):
        try:
            data = self.df
            # Mask all columns except 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
            data = data.drop(columns=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])

            # print(data.info())
            print(data.head(1))
            #Identify discrete (categorical) columns
            # 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
            discrete_columns = ['RatecodeID', 'store_and_fwd_flag', 'ehail_fee']  # Replace with actual column names

            #Initialize and train the CTGAN model
            ctgan = CTGAN()
            ctgan.fit(data, discrete_columns=discrete_columns, epochs=10)

            # Step 4: Generate synthetic data
            synthetic_data = ctgan.sample(num_samples)  # Replace 100 with the desired number of rows

            print(synthetic_data)
            return True
        except Exception as e:
            print(f"Error in generate_sample_data_gans() >> : {e}")
            return False
        
    # generate using SDV is a specialized library for generating high-quality synthetic data based on existing datasets.



# Example usage
if __name__ == "__main__":
    generator = DataGenerator()
    generator.save_sample_data(5)