from kafka import KafkaConsumer
import json
import uuid
import time
from config import KafkaConfig, logger
from hbase_utils import get_connection as get_hbase_connection, create_table_if_not_exists as create_hbase_table, store_comment
from mysql_client import get_connection as get_mysql_connection, create_tables_if_not_exist as create_mysql_tables, insert_sentiment_data
from sentiment_processor import SentimentProcessor

def create_consumer():
    return KafkaConsumer(
        KafkaConfig.TOPIC,
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset=KafkaConfig.AUTO_OFFSET_RESET,
        group_id=KafkaConfig.CONSUMER_GROUP_ID
    )

def main():
    # Set up HBase
    logger.info("Initializing HBase connection...")
    hbase_connection = get_hbase_connection()
    create_hbase_table(hbase_connection)

    # Set up MySQL
    logger.info("Initializing MySQL connection...")
    mysql_connection = get_mysql_connection()
    create_mysql_tables(mysql_connection)

    # Initialize sentiment processor
    logger.info("Initializing sentiment processor...")
    sentiment_processor = SentimentProcessor()

    # Set up Kafka consumer
    consumer = create_consumer()
    try:
        logger.info(f"Starting consumer... listening on topic: {KafkaConfig.TOPIC}")
        for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")

            # Generate a unique row key for HBase using timestamp and UUID
            current_timestamp = int(time.time() * 1000)
            unique_id = uuid.uuid4().hex[:8]
            row_key = f"{current_timestamp}-{unique_id}"

            # Store in HBase
            store_comment(hbase_connection, row_key, data)
            logger.info(f"Stored comment in HBase with row key: {row_key}")

            # Process sentiment
            comment_text = data['comment']
            user_id = data['user_id']
            sentiment = sentiment_processor.process_comment(
                hbase_connection,
                row_key,
                comment_text
            )
            logger.info(f"Sentiment analysis for comment: '{comment_text}' → {sentiment}")

            # Store in MySQL
            try:
                insert_sentiment_data(
                    mysql_connection,
                    row_key,
                    user_id,
                    comment_text,
                    current_timestamp,
                    sentiment
                )
                logger.info(f"Stored sentiment data in MySQL with ID: {row_key}")
            except Exception as e:
                logger.error(f"Error storing sentiment data in MySQL: {e}")

    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        consumer.close()
        hbase_connection.close()
        mysql_connection.close()
        logger.info("Consumer stopped and connections closed.")

if __name__ == "__main__":
    main()