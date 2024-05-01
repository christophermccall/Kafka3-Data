import sqlalchemy
from kafka import KafkaConsumer, TopicPartition
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, mapped_column
from random import randrange
from json import loads
import mysql.connector
import os

# not necessary but ive done this for password safety
host = os.environ["host"]
user = os.environ["user"]
password = os.environ["password"]


Base = sqlalchemy.orm.declarative_base()

# class Customer(Base):  # your class Name should be the same as your table name
#     __tablename__ = 'customer'
#     prim_id = Column(Integer, primary_key=True)
#     id = Column(Integer)
#     balance = Column(Integer)
#     transaction = relationship("Transaction", back_populates="customer")


class Transaction(Base):  # your class Name should be the same as your table name
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True)
    custid = mapped_column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)








engine = create_engine(f'mysql://{user}:{password}@{host}/kafka_example')

# Create all tables in the database
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()










class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        # self.cursor = mydb.cursor()
        # self.create = """CREATE TABLE transaction (id INT,
        #                                     custid INT,
        #                                     type VARCHAR(250),
        #                                     date INT,
        #                                     amt INT,
        #                                     PRIMARY KEY (id));"""
        # self.insert = """INSERT INTO transaction (custid, type, date, amt)
        #                                     VALUES ()
        # """

    # def update_table(self):
    #     self.cursor.execute(self.create)

        #Go back to the readme.

    def handleMessages(self):
        for payload in self.consumer:
            message = payload.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            custid = message.get("custid")
            type = message.get("type")
            date = message.get("date")
            amt = message.get("amt")

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']

            # if not session.query(Customer.id).exists():
            #     balance_entry = Customer(id=custid, balance=self.custBalances[message['custid']])
            #     session.add(balance_entry)
            #     session.commit()
            # else:
            #     costumer = session.execute(select(Customer).filter_by(id=custid)).scalar_one()
            #     costumer.balance = self.custBalances[message['custid']]
            #     session.commit()

            entry = Transaction(custid=custid, type=type, date=date, amt=amt)
            session.add(entry)
            session.commit()
            session.close()

            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
