from kafka import KafkaConsumer
import json
import uuid
import time
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from hbase_utils import get_connection, create_table_if_not_exists, store_comment

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='example-consumer-group'
    )

def main():
    # Set up HBase
    print("Initializing HBase connection...")
    hbase_connection = get_connection()
    create_table_if_not_exists(hbase_connection)

    # Set up Kafka consumer
    consumer = create_consumer()
    try:
        print(f"Starting consumer... listening on topic: {KAFKA_TOPIC}")
        for message in consumer:
            data = message.value
            print(f"Received message: {data}")

            # Generate a unique row key for HBase using timestamp and UUID
            row_key = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

            # Store in HBase
            store_comment(hbase_connection, row_key, data)
            print(f"Stored comment in HBase with row key: {row_key}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        hbase_connection.close()

if __name__ == "__main__":
    main()