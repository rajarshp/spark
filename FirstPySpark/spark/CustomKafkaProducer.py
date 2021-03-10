import json

from bson import json_util
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Preparing to send data to Kafka")
with open('CreditCardApplicationData.csv') as f:
    for line in f:
        data = json.dumps(line, default=json_util.default).encode('utf-8')
        producer.send("carddata", data)

print("Data sent to Kafka")
