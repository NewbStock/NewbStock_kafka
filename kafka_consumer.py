from confluent_kafka import Consumer, KafkaException, KafkaError
import json

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
                # 메시지를 콘솔에 출력
                record = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {record}")
    except KeyboardInterrupt:
        pass
    finally:
        # 종료 시 정리
        consumer.close()

process_messages()
