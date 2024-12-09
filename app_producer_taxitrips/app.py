
import configparser
import time
import random
from scripts import data_generator
from scripts.kafka_producer_consumer import MyKafkaManager
import json
import argparse


def read_config(file_path):
    config = configparser.ConfigParser()
    # Read the configuration file
    config.read(file_path)
    return config


def send_message(producer, topic, message):
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"Message sent: {message}")
    except Exception as e:
        print(f"Failed to send message: {e}")

def main():
    try:
        # Initialize the parser
        parser = argparse.ArgumentParser(description="A sample script to demonstrate argument parsing.")
        # Define arguments
        parser.add_argument("--broker", type=str, required=True, help="kafka broker address")
        parser.add_argument("--port", type=int, required=False, help="port number")

        # Parse arguments
        args = parser.parse_args()
        print(f"Broker: {args.broker}")
        kafka_broker = args.broker + ":" + str(args.port)
       
        
        # Read the configuration file
        config = read_config('config.ini')
        if not(args.broker):
            kafka_broker = config['kafka']['bootstrap_servers']
        kafka_topic = config['kafka']['topic']
        interval_sec = int(config['recurring']['interval_sec'])

        print(f"Kafka broker: {kafka_broker}")
        print(f"Kafka topic: {kafka_topic}")
        print(f"Interval: {interval_sec}")

        # create or get existing topic
        kafka_class_instance = MyKafkaManager(kafka_broker, kafka_topic)
        print(f"broker: {kafka_class_instance.bootstrap_servers}")
        kafka_class_instance.create_topic()

        # taxi data generator
        generator = data_generator.DataGenerator(example_file="sample_data/taxi_tripdata_top1000.csv")
        generator.validate_sample_file()

        while(True):

            # get taxi trip data
            res = generator.generate_sample_data_faker(1)
            

            # send message
            for key, value in enumerate(res):
                # print(f"Key: {key}, Value: {value}")
                kafka_class_instance.send_message(kafka_topic, value)
            

            # wait for interval
            print(f"Waiting for {interval_sec} seconds...")
            time.sleep(interval_sec)
       

      


    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()