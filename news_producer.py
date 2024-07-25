import pandas as pd
from confluent_kafka import Producer
import json
import glob
import os

# Kafka 프로듀서 설정
producer = Producer({'bootstrap.servers': 'localhost:9092'})
# 토픽 이름 설정
topic_name = 'news_to_s3'

# 폴더 안의 모든 CSV 파일을 읽기
EN_temps = glob.glob(os.path.join("EN_temp", '*.csv'))
KR_temps = glob.glob(os.path.join("KR_temp", '*.csv'))

EN_dones = glob.glob(os.path.join("EN_done", '*.csv')) if os.path.exists("EN_done") else []
KR_dones = glob.glob(os.path.join("KR_done", '*.csv')) if os.path.exists("KR_done") else []

def read_csv_files(file_list):
    data = {}
    for file in file_list:
        df = pd.read_csv(file, header=None)
        data[os.path.basename(file)] = df.to_dict(orient='records')
    return data

def process_files(temp_files, done_files):
    temp_data = read_csv_files(temp_files)
    done_data = read_csv_files(done_files)
    
    if not temp_data:
        return {}
    
    if not done_data:
        return temp_data
    
    non_duplicate_data = {}
    for file, records in temp_data.items():
        done_records = done_data.get(file, [])
        non_duplicate_records = [record for record in records if record not in done_records]
        if non_duplicate_records:
            non_duplicate_data[file] = non_duplicate_records
    
    return non_duplicate_data

# EN_temp와 EN_done 파일을 처리
EN_non_duplicates = process_files(EN_temps, EN_dones)
# KR_temp와 KR_done 파일을 처리
KR_non_duplicates = process_files(KR_temps, KR_dones)

# 결과를 Kafka로 전송
def send_to_kafka(data, topic_name):
    for file_name, records in data.items():
        message = {file_name: records}
        producer.produce(topic_name, json.dumps(message))
        producer.poll(0)

send_to_kafka(EN_non_duplicates, topic_name)
send_to_kafka(KR_non_duplicates, topic_name)

# 프로듀서 플러시
producer.flush()
