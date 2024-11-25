from kafka import KafkaConsumer
from configs import kafka_config
import json

# Creating Kafka consumer
consumer = KafkaConsumer(
  bootstrap_servers=kafka_config['bootstrap_servers'],
  security_protocol=kafka_config['security_protocol'],
  sasl_mechanism=kafka_config['sasl_mechanism'],
  sasl_plain_username=kafka_config['username'],
  sasl_plain_password=kafka_config['password'],
  value_deserializer=lambda v: json.loads(v.decode('utf-8')),
  key_deserializer=lambda v: json.loads(v.decode('utf-8')),
  auto_offset_reset='earliest', # Read from start
  enable_auto_commit=True, # Sutomatic confiramtion of read messages
  # group_id='my_consumer_group_3'
)

# Topic name
my_name = 'andriy_b'
temperature_topic=f'{my_name}_temperature_alert'

# Subscribe to topic
consumer.subscribe([temperature_topic])

print(f"Subscribed to topics '{temperature_topic}'")

# Processing messages from topic
try:
  for message in consumer:
    print(f"TEMPERATURE ALERT - Recieved message: {message.value}, with key: {message.key}, partition: {message.partition} ")

except Exception as e:
  print(f'An error occurred: {e}')
finally:
  consumer.close()