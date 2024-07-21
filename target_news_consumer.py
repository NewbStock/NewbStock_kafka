from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time
import os
import csv

# Kafka 컨슈머 설정
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'target_news_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# 토픽 구독
topic_name = 'target_to_news'
consumer.subscribe([topic_name])

def print_message(msg):
    print("Received data: {}".format(msg))

# 메시지 소비
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            message = msg.value().decode('utf-8')
            target_data = json.loads(message)

            country = target_data['country']
            ticker = target_data['ticker']
            name = target_data['name']
            percent = target_data['percent']
            current_time = time.time()

            filename = "Stock timestamp_" + country + ".csv"
            if os.path.exists(filename):
                pass
            else:
                pass


            print_message(name)
except KeyboardInterrupt:
    pass
finally:
    # 컨슈머 닫기
    consumer.close()
