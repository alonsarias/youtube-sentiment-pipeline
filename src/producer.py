from kafka import KafkaProducer
import json
import time
import random
import uuid
import os
import sys
from config import KafkaConfig, AppConfig, logger

def load_comments(file_path):
    """
    Load comments from a JSON file.

    Args:
        file_path (str): Path to the JSON file containing comments

    Returns:
        list: A list of comment strings

    Raises:
        FileNotFoundError: If the comments file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        ValueError: If the file doesn't contain a list of strings
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Comments file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file:
            comments = json.load(file)

        # Validate that comments is a list of strings
        if not isinstance(comments, list):
            raise ValueError("Comments file must contain a JSON array")

        if not all(isinstance(comment, str) for comment in comments):
            raise ValueError("All elements in the comments file must be strings")

        if len(comments) == 0:
            logger.warning("Comments file is empty. Producer will have no comments to send.")

        return comments

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in comments file: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading comments: {str(e)}")
        raise

def create_producer():
    """
    Create and configure a Kafka producer.

    Returns:
        KafkaProducer: Configured Kafka producer instance
    """
    return KafkaProducer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_message(producer, message):
    """
    Send a message to the Kafka topic with error handling.

    Args:
        producer (KafkaProducer): Kafka producer instance
        message (dict): Message to be sent to Kafka
    """
    future = producer.send(KafkaConfig.TOPIC, message)
    try:
        # Block until the message is sent or times out
        future.get(timeout=KafkaConfig.PRODUCER_TIMEOUT)
        logger.info(f"Message sent successfully: {message}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def generate_comment(comments):
    """
    Generate a comment with user information for simulating real-time data.

    Args:
        comments (list): List of available comment strings

    Returns:
        dict: Dictionary containing user_id and comment text
    """
    # Select a random comment from the loaded list
    comment = random.choice(comments)
    # Generate a random user ID
    user_id = f"user_{uuid.uuid4().hex[:8]}"

    return {
        "user_id": user_id,
        "comment": comment
    }

def main():
    """Main function to run the Kafka producer simulation."""
    try:
        # Use the configurable path for the comments file
        comments_file = AppConfig.COMMENTS_FILE_PATH
        logger.info(f"Loading comments from: {comments_file}")

        # Load comments from the JSON file
        comments = load_comments(comments_file)
        logger.info(f"Successfully loaded {len(comments)} comments from {comments_file}")

        producer = create_producer()
        try:
            logger.info(f"Starting producer... sending messages to topic: {KafkaConfig.TOPIC}")
            while True:
                message = generate_comment(comments)
                send_message(producer, message)
                # Random delay between min and max delay to simulate varying load
                delay = random.uniform(AppConfig.PRODUCER_MIN_DELAY, AppConfig.PRODUCER_MAX_DELAY)
                time.sleep(delay)
        except KeyboardInterrupt:
            logger.info("\nStopping producer...")
        finally:
            producer.close()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()