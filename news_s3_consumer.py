from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import pandas as pd
import os
import time
import boto3
import secrets_key

# Kafka Consumer 설정
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'news_group',
    'auto.offset.reset': 'earliest'
}

# Consumer 생성
consumer = Consumer(consumer_conf)
# 구독할 토픽 설정
topic_name = 'news_to_s3'
consumer.subscribe([topic_name])

# 폴더 경로 설정
EN_done_folder = "EN_done"
KR_done_folder = "KR_done"

# 폴더가 존재하지 않으면 생성
os.makedirs(EN_done_folder, exist_ok=True)
os.makedirs(KR_done_folder, exist_ok=True)

session = boto3.Session(
    aws_access_key_id=secrets_key.aws_access_key_id,
    aws_secret_access_key=secrets_key.aws_secret_access_key
)
s3 = session.client('s3')

def save_to_csv(file_name, records):
    df = pd.DataFrame(records)
    if file_name.startswith('EN_'):
        file_path = os.path.join(EN_done_folder, file_name)
    elif file_name.startswith('KR_'):
        file_path = os.path.join(KR_done_folder, file_name)
    else:
        print(f"Unknown file prefix for {file_name}, skipping...")
        return
    df.to_csv(file_path, index=False, header=False)

def upload_to_s3(folder_path, s3_bucket_name):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        s3_key = f"{folder_path}/{file_name}"
        try:
            s3.upload_file(file_path, s3_bucket_name, s3_key)
            print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {file_name} to S3: {e}")

def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                time.sleep(60)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # 메시지 수신 및 출력
            message_value = msg.value().decode('utf-8')
            
            if message_value == "upload":
                upload_to_s3(EN_done_folder, secrets_key.s3_bucket_name)
                upload_to_s3(KR_done_folder, secrets_key.s3_bucket_name)
            else:
                message = json.loads(message_value)
                print("data received")
                for file_name, records in message.items():
                    print(f"File: {file_name}")
                    save_to_csv(file_name, records)
                    for record in records:
                        print(json.dumps(record, ensure_ascii=False, indent=4))
    except KeyboardInterrupt:
        pass
    finally:
        # Consumer 닫기
        consumer.close()

if __name__ == '__main__':
    consume_messages()
