import mysql.connector
import time
import datetime
from config import MySQLConfig, logger

def get_connection():
    """
    Create and return a connection to MySQL with retry mechanism.

    Returns:
        mysql.connector.connection.MySQLConnection: Active MySQL connection

    Raises:
        Exception: If connection fails after maximum retries
    """
    # Add retry mechanism for Docker container startup timing
    max_retries = MySQLConfig.MAX_RETRIES
    retry_delay = MySQLConfig.RETRY_DELAY

    for i in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host=MySQLConfig.HOST,
                port=MySQLConfig.PORT,
                database=MySQLConfig.DATABASE,
                user=MySQLConfig.USER,
                password=MySQLConfig.PASSWORD,
                auth_plugin=MySQLConfig.AUTH_PLUGIN
            )
            logger.info(f"Successfully connected to MySQL at {MySQLConfig.HOST}:{MySQLConfig.PORT}")
            return connection
        except Exception as e:
            if i < max_retries - 1:
                logger.warning(f"Error connecting to MySQL. Retry {i+1} of {max_retries}. Error: {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to MySQL after {max_retries} attempts: {e}")
                raise

def create_tables_if_not_exist(connection):
    """
    Create the necessary tables for sentiment analysis if they don't exist.

    Args:
        connection: Active MySQL connection

    Raises:
        Exception: If table creation fails
    """
    try:
        cursor = connection.cursor()

        # Create comments table to store sentiment analysis results
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
        logger.info("MySQL tables created or already exist")
    except Exception as e:
        logger.error(f"Error creating MySQL tables: {e}")
        raise
    finally:
        cursor.close()

def insert_sentiment_data(connection, comment_id, user_id, comment_text, timestamp, sentiment):
    """
    Insert sentiment analysis data into MySQL for reporting and visualization.

    Args:
        connection: Active MySQL connection
        comment_id (str): Unique identifier for the comment (matches HBase row key)
        user_id (str): User identifier
        comment_text (str): Original comment text content
        timestamp (int): Unix timestamp in milliseconds
        sentiment (str): Sentiment prediction label from the analysis

    Raises:
        Exception: If insert operation fails
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

        logger.info(f"Inserted sentiment data into MySQL: {comment_id}, sentiment: {sentiment}")
    except Exception as e:
        logger.error(f"Error inserting sentiment data into MySQL: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()