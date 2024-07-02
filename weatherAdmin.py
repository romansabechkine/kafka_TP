# We should reproduce this command but in Python
# cd opt/kafka_2.12-2.3.0/bin && ./kafka-topics.sh --create --topic $TOPIC --partitions 1 --replication-factor 1 --bootstrap-server $BOOTSTRAP
import os

from dotenv import load_dotenv
import kafka
from kafka.admin import KafkaAdminClient, NewTopic


def weatherAdmin():
    load_dotenv()
    BOOTSTRAP=os.getenv('BOOTSTRAP')
    TOPIC=os.getenv('TOPIC') 


    class KafkaAdmin():

        def __init__(self, bootstrap):
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap,
            )

        def createTopic(self, topic_list):
            exisiting_topics = self.admin_client.list_topics()
            for t in topic_list:
                if t in exisiting_topics:
                    print("LOG Topic Already exist!")
                    continue
            try:
                self.admin_client.create_topics(new_topics=topic_list)
            except kafka.errors.TopicAlreadyExistsError:
                print("Topic Already exist!")
                pass
            
    topic_list = []
    topics = dict(
        topic_paris = "weather-paris",
        topic_nice = "weather-nice",
        topic_rennes = "weather-rennes",
        topic_grenoble = "weather-grenoble",
        topic_bordeaux = "weather-bordeaux"
    )

    for topic in topics.values():
        topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
    adminClient = KafkaAdmin(BOOTSTRAP)
    adminClient.createTopic(topic_list = topic_list)


if __name__ == '__main__':
    weatherAdmin()
