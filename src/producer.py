from kafka import KafkaProducer
import json
import time
import random
import uuid
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

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
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_message(producer, message):
    future = producer.send(KAFKA_TOPIC, message)
    try:
        future.get(timeout=10)
        print(f"Message sent successfully: {message}")
    except Exception as e:
        print(f"Error sending message: {e}")

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
        print(f"Starting producer... sending messages to topic: {KAFKA_TOPIC}")
        while True:
            message = generate_comment()
            send_message(producer, message)
            # Random delay between 0.5 and 3.0 seconds
            delay = random.uniform(0.5, 3.0)
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()