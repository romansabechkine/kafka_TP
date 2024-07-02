import datetime
import json
import logging
from socket import timeout
import threading
import time
from urllib.error import HTTPError, URLError
import urllib.request
import os
from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()
URL = os.getenv('URL')
BOOTSTRAP=os.getenv('BOOTSTRAP')
TOPIC = os.getenv('TOPIC')


class KafkaProd(threading.Thread):

  def __init__(self, topic, url):
    threading.Thread.__init__(self)
    self.producer = KafkaProducer(
      bootstrap_servers=BOOTSTRAP,
      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
      compression_type='gzip')
    self.topic = topic
    self.url = url

  def run(self):
      try:
        while True:
            response  = urllib.request.urlopen(self.url, timeout=10)
            data = json.loads(response.read().decode())
            data_to_kafka = {
              "city": data["location"]["name"],
              "datetime" : datetime.datetime.now().isoformat(timespec='minutes'),
              "temperature" : data["current"]["temp_c"],
              "wind_kph": data["current"]["wind_kph"],
              "humidity": data["current"]["humidity"]
            }
            self.producer.send(self.topic, data_to_kafka)
            self.producer.flush()
            print("LOG Produced: {} ".format(data_to_kafka))
          
            # make record every 60 seconds
            time.sleep(60)
      except HTTPError as error:
        logging.error('HTTP Error: Data of %s not retrieved because of: %s', self.url, error)
      except URLError as error:
        if isinstance(error.reason, timeout):
          logging.error('Timeout Error: Data of %s not retrieved because of: %s', self.url, error)
        else:
          logging.error('URL Error: Data of %s not retrieved because of: %s', self.url, error)
      except AttributeError as error:
        logging.error('Attribute error for %s because of: %s', self.url, error)
        
        









