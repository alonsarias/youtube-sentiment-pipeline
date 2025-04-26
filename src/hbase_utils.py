import happybase
import time
from config import HBaseConfig, logger

def get_connection():
    """
    Create and return a connection to HBase with retry mechanism.

    Returns:
        happybase.Connection: Active HBase connection

    Raises:
        Exception: If connection fails after max retries
    """
    max_retries = HBaseConfig.MAX_RETRIES
    retry_delay = HBaseConfig.RETRY_DELAY

    for i in range(max_retries):
        try:
            connection = happybase.Connection(host=HBaseConfig.HOST, port=HBaseConfig.PORT)
            logger.info(f"Successfully connected to HBase at {HBaseConfig.HOST}:{HBaseConfig.PORT}")
            return connection
        except Exception as e:
            if i < max_retries - 1:
                logger.warning(f"Error connecting to HBase. Retry {i+1} of {max_retries}. Error: {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to HBase after {max_retries} attempts: {e}")
                raise

def create_table_if_not_exists(connection):
    """
    Create the HBase table if it doesn't exist.

    Args:
        connection (happybase.Connection): Active HBase connection
    """
    tables = connection.tables()
    tables = [t.decode('utf-8') for t in tables]  # Convert bytes to strings

    if HBaseConfig.TABLE_NAME not in tables:
        logger.info(f"Creating table: {HBaseConfig.TABLE_NAME}")
        connection.create_table(
            HBaseConfig.TABLE_NAME,
            {HBaseConfig.COLUMN_FAMILY: dict()}
        )
        logger.info(f"Table {HBaseConfig.TABLE_NAME} created successfully")
    else:
        logger.info(f"Table {HBaseConfig.TABLE_NAME} already exists")

def store_comment(connection, row_key, data):
    """
    Store a comment in HBase table.

    Args:
        connection (happybase.Connection): Active HBase connection
        row_key (str): Unique identifier for the row
        data (dict): Dictionary containing at minimum 'user_id' and 'comment' keys
    """
    table = connection.table(HBaseConfig.TABLE_NAME)

    # Convert all values to bytes for HBase storage
    mapped_data = {
        f"{HBaseConfig.COLUMN_FAMILY}:timestamp": str(int(time.time())).encode('utf-8'),
        f"{HBaseConfig.COLUMN_FAMILY}:user_id": data['user_id'].encode('utf-8'),
        f"{HBaseConfig.COLUMN_FAMILY}:comment": data['comment'].encode('utf-8'),
        f"{HBaseConfig.COLUMN_FAMILY}:processed": b"False"
    }

    table.put(row_key.encode('utf-8'), mapped_data)

def update_comment_with_sentiment(connection, row_key, sentiment):
    """
    Update a comment in HBase with sentiment analysis results.

    Args:
        connection (happybase.Connection): Active HBase connection
        row_key (str): Unique identifier for the row
        sentiment (str): Sentiment label determined by the analysis
    """
    table = connection.table(HBaseConfig.TABLE_NAME)

    mapped_data = {
        f"{HBaseConfig.COLUMN_FAMILY}:sentiment": sentiment.encode('utf-8'),
        f"{HBaseConfig.COLUMN_FAMILY}:processed": b"True",
        f"{HBaseConfig.COLUMN_FAMILY}:processed_timestamp": str(int(time.time())).encode('utf-8')
    }

    table.put(row_key.encode('utf-8'), mapped_data)

def get_comment(connection, row_key):
    """
    Retrieve a comment from HBase table.

    Args:
        connection (happybase.Connection): Active HBase connection
        row_key (str): Unique identifier for the row

    Returns:
        dict: Dictionary containing the comment data with decoded string values
    """
    table = connection.table(HBaseConfig.TABLE_NAME)

    # Get the row data
    row = table.row(row_key.encode('utf-8'))

    # Convert bytes back to strings
    result = {}
    for key, value in row.items():
        column_name = key.decode('utf-8').split(':')[1]
        result[column_name] = value.decode('utf-8')

    return result