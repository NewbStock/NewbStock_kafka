import pandas as pd
from confluent_kafka import Producer
import json
import os
import time

# Kafka 프로듀서 설정
producer = Producer({'bootstrap.servers': 'localhost:9092'})

while True:
    # 첫 번째 CSV 파일 읽기
    csv_file_1 = 'Target Stock List_EN.csv'
    if os.path.exists(csv_file_1):
        df1 = pd.read_csv(csv_file_1, encoding='utf-8', header=None)
        df1.columns = ['country', 'ticker', 'name', 'percent', 'time']
    else:
        df1 = pd.DataFrame(columns=['country', 'ticker', 'name', 'percent', 'time'])

    # 두 번째 CSV 파일 읽기
    csv_file_2 = 'Target Stock List_KR.csv'
    if os.path.exists(csv_file_2):
        df2 = pd.read_csv(csv_file_2, encoding='utf-8', header=None)
        df2.columns = ['country', 'ticker', 'name', 'percent', 'time']
    else:
        df2 = pd.DataFrame(columns=['country', 'ticker', 'name', 'percent', 'time'])

    # 두 데이터프레임 합치기
    df_combined = pd.concat([df1, df2], ignore_index=True)

    # 토픽 이름 설정
    topic_name = 'target_to_news'

    # 메시지 전송 콜백 함수
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    # 합쳐진 데이터프레임을 한 줄씩 Kafka로 전송
    for index, row in df_combined.iterrows():
        # 데이터를 JSON으로 변환
        message = row.to_json(force_ascii=False)
        # Kafka로 메시지 전송
        producer.produce(topic_name, value=message, callback=delivery_report)
        producer.poll(1)

    # 프로듀서 플러시
    producer.flush()
    time.sleep(600)
