import subprocess
import concurrent.futures
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_simple_kafka_topics(topic_names, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",  # Kafka 서버 주소
        client_id='test_client'
    )

    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor) for topic_name in topic_names]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError as e:
        pass
    finally:
        admin_client.close()

# 예제 사용
topics_to_create = ["target_to_news", "news_to_s3", "stock-prices"]

# 각 스크립트를 순차적으로 실행
scripts = [
    "en_stock_producer.py",
    "kr_stock_producer.py",
    "target_producer.py",
    "news_producer.py",
    "news_s3_consumer.py",
    "stock_target_consumer.py",
    "target_news_consumer.py",
]

def run_script(script):
    result = subprocess.run(["python3", script], capture_output=True, text=True)
    if result.returncode != 0:
        return f"Error executing {script}: {result.stderr}"
    else:
        return f"{script} Done"

if __name__ == "__main__":
    # Kafka 토픽 생성
    create_simple_kafka_topics(topics_to_create)

    # ProcessPoolExecutor를 사용하여 병렬로 실행
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(run_script, scripts))

    for result in results:
        print(result)
