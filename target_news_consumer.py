from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time
import os
import csv
import urllib.request
import secrets_key

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

def print_message2(msg):
    try:
        message = msg.value().decode('utf-8')
        target_data = json.loads(message)
        print(f"Received data: {json.dumps(target_data, indent=4, ensure_ascii=False)}")
    except Exception as e:
        print(f"Error decoding message: {e}")

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

            filename = "Stock_timestamp_" + country + ".csv"
            news = False
            find = False
            if os.path.exists(filename):
                with open(filename, "r", encoding='utf-8') as file:
                    reader = csv.reader(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    rows = list(reader)
                for row in rows:
                    if str(row[1]) == str(ticker):
                        find = True
                        if current_time - float(row[4]) >= 300:
                            row[4] = current_time
                            news = True
                        break
                if not find:
                    rows.append([country, ticker, name, percent, current_time])
                    news = True
            else:
                rows = [[country, ticker, name, percent, current_time]]
                news = True

            with open(filename, "w", newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerows(rows)
            
            # 여기서부터 뉴스 끌어오기
            if news:
                query = name
                sort = "date"  # 정렬 옵션: sim (유사도 순), date (날짜 순)
                start = 1  # 검색 시작 위치
                display = 10  # 가져올 검색 결과 수
                encText = urllib.parse.quote(query)
                url = f"https://openapi.naver.com/v1/search/news?query={encText}&sort={sort}&start={start}&display={display}"
                request = urllib.request.Request(url)
                request.add_header("X-Naver-Client-Id", secrets_key.client_key)
                request.add_header("X-Naver-Client-Secret", secrets_key.client_secret)
                response = urllib.request.urlopen(request)
                rescode = response.getcode()

                if rescode == 200:
                    response_body = response.read()
                    news_data = json.loads(response_body.decode('utf-8'))
                    news_items = news_data["items"]

                    sub_folder = country + "_temp"
                    if not os.path.exists(sub_folder):
                        os.makedirs(sub_folder)
                    news_filename = os.path.join(sub_folder, country + "_" + str(ticker) + "_temp.csv")

                    existing_news = []
                    if os.path.exists(news_filename):
                        with open(news_filename, "r", newline='', encoding='utf-8') as news_file:
                            news_reader = csv.reader(news_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            existing_news = list(news_reader)
                    
                    new_news = []
                    for news_item in news_items:
                        new_row = [news_item['title'], news_item['originallink'], news_item['link'], news_item['description'], news_item['pubDate']]
                        if new_row not in existing_news:
                            new_news.append(new_row)

                    with open(news_filename, "a", newline='', encoding='utf-8') as news_file:
                        writer = csv.writer(news_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        writer.writerows(new_news)

                    print_message(name)
                else:
                    print("Error Code:" + rescode)

except KeyboardInterrupt:
    pass
finally:
    # 컨슈머 닫기
    consumer.close()
