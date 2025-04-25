import mysql.connector
import time
import datetime
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD

def get_connection():
    """Create and return a connection to MySQL."""
    # Add retry mechanism for Docker container startup timing
    max_retries = 5
    retry_delay = 5  # seconds

    for i in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                auth_plugin='mysql_native_password'  # Added auth_plugin to fix RSA public key error
            )
            print(f"Successfully connected to MySQL at {MYSQL_HOST}:{MYSQL_PORT}")
            return connection
        except Exception as e:
            if i < max_retries - 1:
                print(f"Error connecting to MySQL. Retry {i+1} of {max_retries}. Error: {e}")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to MySQL after {max_retries} attempts: {e}")
                raise

def create_tables_if_not_exist(connection):
    """Create the necessary tables if they don't exist."""
    try:
        cursor = connection.cursor()

        # Create comments table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id VARCHAR(36) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            comment TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            sentiment_prediction VARCHAR(50) NOT NULL
        )
        """)

        connection.commit()
        print("MySQL tables created or already exist")
    except Exception as e:
        print(f"Error creating MySQL tables: {e}")
        raise
    finally:
        cursor.close()

def insert_sentiment_data(connection, comment_id, user_id, comment_text, timestamp, sentiment):
    """
    Insert sentiment analysis data into MySQL.

    Args:
        connection: MySQL connection
        comment_id: Unique identifier for the comment
        user_id: User identifier
        comment_text: Comment text content
        timestamp: Unix timestamp (milliseconds)
        sentiment: Sentiment prediction label
    """
    try:
        cursor = connection.cursor()

        # Convert Unix timestamp (milliseconds) to datetime object
        datetime_obj = datetime.datetime.fromtimestamp(timestamp / 1000.0)

        query = """
        INSERT INTO comments (id, user_id, comment, timestamp, sentiment_prediction)
        VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(query, (comment_id, user_id, comment_text, datetime_obj, sentiment))
        connection.commit()

        print(f"Inserted sentiment data into MySQL: {comment_id}, sentiment: {sentiment}")
    except Exception as e:
        print(f"Error inserting sentiment data into MySQL: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()