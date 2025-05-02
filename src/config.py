"""
Configuration module for the Sentiment Analysis Big Data Simulation.

This module loads configuration from environment variables and provides
structured access to all configurable parameters used throughout the application.
Default values are provided when environment variables are not set.
"""
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging based on environment variable
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("Loading configuration...")

# ------------------------------
# Kafka Configuration
# ------------------------------
class KafkaConfig:
    """Kafka connection and operational parameters."""
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'comments')
    CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'example-consumer-group')
    AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    PRODUCER_TIMEOUT = int(os.getenv('KAFKA_PRODUCER_TIMEOUT', 10))

# ------------------------------
# HBase Configuration
# ------------------------------
class HBaseConfig:
    """HBase connection and table parameters."""
    HOST = os.getenv('HBASE_HOST', 'localhost')
    PORT = int(os.getenv('HBASE_PORT', 9090))
    TABLE_NAME = os.getenv('HBASE_TABLE_NAME', 'comments')
    COLUMN_FAMILY = os.getenv('HBASE_COLUMN_FAMILY', 'data')
    MAX_RETRIES = int(os.getenv('HBASE_MAX_RETRIES', 5))
    RETRY_DELAY = int(os.getenv('HBASE_RETRY_DELAY', 5))

# ------------------------------
# MySQL Configuration
# ------------------------------
class MySQLConfig:
    """MySQL database connection and operational parameters."""
    HOST = os.getenv('MYSQL_HOST', 'localhost')
    PORT = int(os.getenv('MYSQL_PORT', 3306))
    DATABASE = os.getenv('MYSQL_DATABASE', 'sentiment_analysis')
    USER = os.getenv('MYSQL_USER', 'user')
    PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
    AUTH_PLUGIN = os.getenv('MYSQL_AUTH_PLUGIN', 'mysql_native_password')
    MAX_RETRIES = int(os.getenv('MYSQL_MAX_RETRIES', 5))
    RETRY_DELAY = int(os.getenv('MYSQL_RETRY_DELAY', 5))

# ------------------------------
# Sentiment Analysis Configuration
# ------------------------------
class SentimentConfig:
    """Configuration for sentiment analysis model and processing."""
    MODEL_NAME = os.getenv('SENTIMENT_MODEL_NAME', 'tabularisai/multilingual-sentiment-analysis')
    MAX_LENGTH = int(os.getenv('SENTIMENT_MAX_LENGTH', 512))
    # Map model outputs (integers) to human-readable sentiment labels
    SENTIMENT_MAP = {
        0: "Very Negative",
        1: "Negative",
        2: "Neutral",
        3: "Positive",
        4: "Very Positive"
    }

# ------------------------------
# YouTube Configuration
# ------------------------------
class YouTubeConfig:
    """YouTube API and live chat configuration parameters."""
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    POLL_INTERVAL = int(os.getenv('YOUTUBE_POLL_INTERVAL', 5))
    MAX_RESULTS = int(os.getenv('YOUTUBE_MAX_RESULTS', 200))

# ------------------------------
# Application Settings
# ------------------------------
class AppConfig:
    """General application settings."""
    PRODUCER_MIN_DELAY = float(os.getenv('PRODUCER_MIN_DELAY', 0.5))
    PRODUCER_MAX_DELAY = float(os.getenv('PRODUCER_MAX_DELAY', 3.0))
    # Construct absolute path to comments file based on project structure
    COMMENTS_FILE_PATH = os.getenv('COMMENTS_FILE_PATH', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'comments.json'))

# For backward compatibility, maintain the original constants at the module level
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = KafkaConfig.BOOTSTRAP_SERVERS
KAFKA_TOPIC = KafkaConfig.TOPIC

# HBase configuration
HBASE_HOST = HBaseConfig.HOST
HBASE_PORT = HBaseConfig.PORT
HBASE_TABLE_NAME = HBaseConfig.TABLE_NAME
HBASE_COLUMN_FAMILY = HBaseConfig.COLUMN_FAMILY

# MySQL configuration
MYSQL_HOST = MySQLConfig.HOST
MYSQL_PORT = MySQLConfig.PORT
MYSQL_DATABASE = MySQLConfig.DATABASE
MYSQL_USER = MySQLConfig.USER
MYSQL_PASSWORD = MySQLConfig.PASSWORD

# Log configuration info (excluding sensitive info)
logger.info(f"Kafka Topic: {KAFKA_TOPIC}, Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"HBase Host: {HBASE_HOST}, Port: {HBASE_PORT}, Table: {HBASE_TABLE_NAME}")
logger.info(f"MySQL Host: {MYSQL_HOST}, Port: {MYSQL_PORT}, Database: {MYSQL_DATABASE}")
logger.info(f"Sentiment Model: {SentimentConfig.MODEL_NAME}")
logger.info(f"YouTube Poll Interval: {YouTubeConfig.POLL_INTERVAL}s")