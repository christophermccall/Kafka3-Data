from kafka import KafkaProducer
import csv
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open('/Users/chris/pyprojects/Kafka3-Data/event_test_data_set.csv', 'r') as file:
    reader = csv.DictReader(file)
    for line in reader:
        producer.send(topic='user-activity', value=line)
    producer.flush()
    producer.close()




