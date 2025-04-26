from kafka import KafkaProducer
import json
import time
import random
import uuid
from config import KafkaConfig, AppConfig, logger

# Define comments as a flat list with mixed sentiments
COMMENTS = [
    # Very Negative
    "I hate this!",
    "This is awful",
    "Terrible experience",
    "Worst service ever",
    "Completely disappointed",
    # Negative
    "Not great",
    "Could be better",
    "I'm not satisfied",
    "Below expectations",
    "Somewhat disappointing",
    # Neutral
    "It's okay",
    "Average",
    "No strong feelings",
    "Neither good nor bad",
    "Acceptable",
    # Positive
    "This is nice",
    "I like it",
    "Good work",
    "Pleasant experience",
    "Better than expected",
    # Very Positive
    "Absolutely love it!",
    "Fantastic!",
    "Best ever",
    "Exceptional quality",
    "Outstanding performance"
]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_message(producer, message):
    future = producer.send(KafkaConfig.TOPIC, message)
    try:
        future.get(timeout=KafkaConfig.PRODUCER_TIMEOUT)
        logger.info(f"Message sent successfully: {message}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def generate_comment():
    # Select a random comment from the flat list
    comment = random.choice(COMMENTS)
    # Generate a random user ID
    user_id = f"user_{uuid.uuid4().hex[:8]}"

    return {
        "user_id": user_id,
        "comment": comment
    }

def main():
    producer = create_producer()
    try:
        logger.info(f"Starting producer... sending messages to topic: {KafkaConfig.TOPIC}")
        while True:
            message = generate_comment()
            send_message(producer, message)
            # Random delay between min and max delay
            delay = random.uniform(AppConfig.PRODUCER_MIN_DELAY, AppConfig.PRODUCER_MAX_DELAY)
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()