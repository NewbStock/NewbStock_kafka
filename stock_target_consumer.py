from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import os
import csv

# Kafka 컨슈머 설정
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'price-alert-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['stock-prices'])

def process_messages():
    """Kafka 메시지를 처리하여 콘솔에 출력"""
    try:
        while True:
            msg = consumer.poll(1.0)  # 1초 동안 대기하여 메시지 폴링
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 메시지 끝에 도달
                    print(f"End of partition reached {msg.partition()} {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # 메시지 수신
                message = msg.value().decode('utf-8')
                
                # JSON 파싱
                stock_data = json.loads(message)
                
                # 데이터 처리
                country = stock_data['country']
                symbol = stock_data['symbol']
                name = stock_data['name']
                sector = stock_data['sector']
                price = stock_data['price']
                percent = stock_data['percent']
                high_price = stock_data['stck_hgpr']
                low_price = stock_data['stck_lwpr']
                timestamp = stock_data['timestamp']

                print(f"Received data: {symbol}")
                filename = "Target Stock List_" + country + ".csv"
                if percent < 0.95 and percent != 0:
                    symbol_found = False
                    rows = []

                    if os.path.exists(filename):
                        with open(filename, "r", encoding='utf-8') as file:
                            reader = csv.reader(file)
                            rows = list(reader)

                        # symbol이 이미 있는지 확인
                        for row in rows:
                            if row[1] == symbol:
                                symbol_found = True
                                break

                    if not symbol_found:
                        # 새로운 symbol 추가
                        rows.append([country, symbol, name, percent, 0])

                    # CSV 파일에 쓰기
                    with open(filename, "w", newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerows(rows)
    except KeyboardInterrupt:
        pass
    finally:
        # 종료 시 정리
        consumer.close()

process_messages()
