# This producer would generate a message for every 10 seconds. Which contain the timestamp of
import os
import time
from datetime import datetime

from nops_kafka import Producer
from nops_kafka import ensure_topics

bootstrap_servers = os.environ["KAFKA_BOOSTRAP_SERVERS"]
topic_name = "testing.kafka.topic.databricks"

producer = Producer(bootstrap_servers=bootstrap_servers)
ensure_topics(
    bootstrap_servers=bootstrap_servers, required_topics=[topic_name], num_partitions=1
)

while True:
    now_time = str(datetime.now())
    producer.send(
        topic_name, value={"now_time": now_time}, headers={"event_name": "metrics"}
    )
    producer.send(
        topic_name, value={"now_time": now_time}, headers={"event_name": "metadata"}
    )
    producer.flush()
    time.sleep(10)
