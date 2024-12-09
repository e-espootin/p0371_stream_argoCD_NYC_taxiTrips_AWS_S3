from datetime import datetime
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
import json
import pandas as pd
import boto3
from io import StringIO

class MyKafkaManager:
    def __init__(self, bootstrap_servers=['kafka:9092'], topic_name = "taxi-topic", aws_access_key_id='x', aws_secret_access_key='y', bucket_name="ebi-generalpurpose-bucket", s3_file_path = "p037"):    
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.topic_name = topic_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_key = aws_secret_access_key
        self.bucket_name = bucket_name
        self.s3_file_path = s3_file_path

    def create_producer(self):
        try:
            pass
        except Exception as e:
            print(f"Failed to create producer: {e}")

    def create_topic(self):
        try:
            pass
        except Exception as e:
            print(f"Failed to create topic: {e}")
        
            

        
    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def export_messages_as_parquet(self, messages: list):
        try:
            # convert messages to DataFrame
            data = []
            for message in messages:
                data.append(json.loads(message))


            df = pd.DataFrame(data)
            # print(f"DataFrame: {df.info()}")

            # file name with date - time stamp
            local_path = '~/Downloads/test_output_parquet/'
            filename = f'output_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
            print("{}/{}".format(local_path, filename))
            #df.to_parquet("{}/{}".format(local_path, filename))

            
            # Initialize S3 client
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_key,
            )
            # Convert DataFrame to CSV in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload CSV to S3
            target_filename = f"{self.s3_file_path}/{filename}"
            s3.put_object(Bucket=self.bucket_name, Key=target_filename, Body=csv_buffer.getvalue())

            print(f"DataFrame successfully written to s3://{self.bucket_name}/{target_filename}")
        
        except Exception as e:
            print(f"Failed to store messages!! : {e}")

    def send_message(self, topic, message: json):
        try:
            if not self.producer:
                print(f"Creating producer for topic: {topic}")
                self.create_producer()

            # send message
            # for data in message:
            print(f"Sending message: {message}")
            # Trigger any available delivery report callbacks from previous produce() calls
            self.producer.poll(0)

            #
            data = json.dumps(message)
            # Asynchronously produce a message. The delivery report callback will
            # be triggered from the call to poll() above, or flush() below, when the
            # message has been successfully delivered or failed permanently.
            future = self.producer.produce(self.topic_name, data.encode('utf-8'), callback=self.delivery_report)

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer.flush()
            print(f"Message has been sent successfuly!")
            return future
        except Exception as e: 
            print(f"Failed to send message: {e}")
            

    def create_consumer(self, group_id='mygroup', auto_offset_reset='earliest', auto_commit_enable=True):
        print(f"Creating consumer for topic: {self.topic_name}")
        self.consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': auto_offset_reset,
                # 'enable.auto.commit': auto_commit_enable,
            })
        #self.consumer.subscribe([self.topic_name])
        
        print(f"Consumer for topic: {self.topic_name} created successfully")
        return self.consumer
    

    def consume_messages_Commit_manually(self, messages_batch_size):
        consumer = self.consumer
        consumer.subscribe([self.topic_name])
        messages = []

        try:
            while True:
                msg = consumer.poll(1.0)  # Poll for messages with a 1-second timeout

                if msg is None:
                    continue  # No message, poll again

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition: {msg.topic()}[{msg.partition()}]")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Process the message
                    messages.append(msg.value().decode('utf-8'))
                    print(f"message length: {len(messages)}, time is: {datetime.now()}")
                    # Commit offset after processing
                    consumer.commit(msg)

                
                # export messages to parquet
                if len(messages) >= messages_batch_size:
                    print(f"Exporting {len(messages)} messages to parquet...")
                    self.export_messages_as_parquet(messages)
                    messages = []

                
            
        except KeyboardInterrupt:
            print("Shutdown requested by user.")
        finally:
            consumer.close()
           
    
    
    def close(self):
        if self.consumer:
            self.consumer.close()
        # if self.admin_client:
        #     self.admin_client.close()
