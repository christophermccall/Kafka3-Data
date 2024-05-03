from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def produce():
    with open('/Users/chris/pyprojects/Kafka3-Data/archive/small_listen.json', 'r') as file:
        for line in file:
            json.loads(line)
            producer.send(topic='listen-activity', value=line)
            print(str(line))
            time.sleep(1)
        producer.close()



produce()

