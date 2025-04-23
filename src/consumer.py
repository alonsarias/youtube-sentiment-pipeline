from kafka import KafkaConsumer
import json
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='example-consumer-group'
    )

def main():
    consumer = create_consumer()
    try:
        print(f"Starting consumer... listening on topic: {KAFKA_TOPIC}")
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()