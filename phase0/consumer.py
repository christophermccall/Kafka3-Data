from kafka import KafkaConsumer
from json import loads
import mysql.connector
import os


host = os.environ["host"]
user = os.environ["user"]
password = os.environ["password"]


mydb = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database="kafka_example"
)


my_cursor = mydb.cursor()
try:
    my_cursor.execute("CREATE DATABASE kafka_example")

except:
    print("Database already exists!")


my_cursor.execute("""CREATE TABLE transaction (id INT,
                                            custid INT,
                                            type VARCHAR(250),
                                            date INT,
                                            amt INT,
                                            PRIMARY KEY (id));""")
#
#
# try:
#     my_cursor.execute("USE kafka_example")
#     my_cursor.execute("CREATE TABLE messages (id INT,content VARCHAR(255), PRIMARY KEY (id))")
# except:
#     print('Table already exists!')



consumer = KafkaConsumer(
    'test',
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda m: loads(m.decode('ascii')))

for message in consumer:
    print(message)
    message = message.value

    print('{} found'.format(message))
