import os
import threading
import time

from dotenv import load_dotenv
from dash_app import run_dash_app
from weatherConsumer import KafkaCons
from weatherProducer import KafkaProd
from weatherAdmin import weatherAdmin


def weatherLaunch():
    load_dotenv()
    BOOTSTRAP=os.getenv('BOOTSTRAP')
    TOPIC = os.getenv('TOPIC')
    MONGO_DB = os.getenv('MONGO_DB')

    url_paris = os.getenv('URL_PARIS')
    url_bordeaux = os.getenv('URL_BORDEAUX')
    url_nice = os.getenv('URL_NICE')
    url_rennes = os.getenv('URL_RENNES')
    url_grenoble= os.getenv('URL_GRENOBLE')


    topic_paris = "weather-paris"
    topic_nice = "weather-nice"
    topic_rennes = "weather-rennes"
    topic_grenoble = "weather-grenoble"
    topic_bordeaux = "weather-bordeaux"

    urls = [url_paris, url_bordeaux, url_nice, url_rennes, url_grenoble]
    topics = [topic_paris, topic_bordeaux, topic_nice, topic_rennes, topic_grenoble]

    dash_thread = threading.Thread(target=run_dash_app)

    producer_threads = []
    consumer_threads = []
    for topic, url in zip(topics, urls):
        print("LOG: Tread launched for %s" % topic)
        p = KafkaProd(topic, url)
        c = KafkaCons(topic, "weather", "weather-collection")

        producer_threads.append(p)
        consumer_threads.append(c)

        p.start()
        c.start()

    dash_thread.start()
    

    for p, c in zip(producer_threads, consumer_threads):
        p.join()
        c.join()
    
    dash_thread.join()

    print("Threading done!")


# function based threading, we use class base threading instead
"""def producer_thread():
    p = KafkaProd(BOOTSTRAP)
    p.ingest(TOPIC, url)
    print("Producer Thread working ...")


def consumer_thread():
    k = KafkaCons(BOOTSTRAP)
    k.ingest()
    print("Consumer Thread working ...")"""


if __name__ =="__main__":
    weatherAdmin()
    time.sleep(5)
    weatherLaunch()

