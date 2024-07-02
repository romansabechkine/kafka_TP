import os
import json
import threading
import time
import pymongo
import urllib.parse
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient



load_dotenv()
URL = os.getenv('URL')
BROKER=os.getenv('BROKER')
TOPIC=os.getenv('TOPIC')
GROUP=os.getenv('GROUP')
BOOTSTRAP=os.getenv('BOOTSTRAP')

MONGO_USER = os.getenv('MONGO_USER')
MONGO_PWD = os.getenv('MONGO_PWD')
MONGO_IP = os.getenv('MONGO_IP')
MONGO_PORT = os.getenv('MONGO_PORT')

mongo_username = urllib.parse.quote_plus(MONGO_USER)
mongo_password = urllib.parse.quote_plus(MONGO_PWD)
mongo_ip = urllib.parse.quote_plus(MONGO_IP)
mongo_port = urllib.parse.quote_plus(MONGO_PORT)

mongo_url = 'mongodb://%s:%s@%s:%s/' % (mongo_username , mongo_password, mongo_ip, mongo_port)

class KafkaCons(threading.Thread):

  def __init__(self, topic, db, collection):
    threading.Thread.__init__(self)
    self.consumer = KafkaConsumer(
      topic,
      bootstrap_servers=BOOTSTRAP,
      value_deserializer=lambda v: json.loads(v.decode('utf-8'),
        ),
      )
    self.client = MongoClient(mongo_url)
    print("LOG: Mongo Client : %s" % self.client)
    print("LOG Consumer: %s" % self.consumer)
    self.db = self.client[f'{db}']
    self.collection_paris = self.db[f'{collection}']

  def run(self):
    for message in self.consumer:
      message_value = self.collection_paris.insert_one(message.value)
      print("LOG Consumed: {}".format(message_value))



