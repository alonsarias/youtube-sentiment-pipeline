import happybase
import time
from config import HBASE_HOST, HBASE_PORT, HBASE_TABLE_NAME, HBASE_COLUMN_FAMILY

def get_connection():
    """Create and return a connection to HBase."""
    # Add retry mechanism for Docker container startup timing
    max_retries = 5
    retry_delay = 5  # seconds

    for i in range(max_retries):
        try:
            connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
            print(f"Successfully connected to HBase at {HBASE_HOST}:{HBASE_PORT}")
            return connection
        except Exception as e:
            if i < max_retries - 1:
                print(f"Error connecting to HBase. Retry {i+1} of {max_retries}. Error: {e}")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to HBase after {max_retries} attempts: {e}")
                raise

def create_table_if_not_exists(connection):
    """Create the HBase table if it doesn't exist."""
    tables = connection.tables()
    tables = [t.decode('utf-8') for t in tables]  # Convert bytes to strings

    if HBASE_TABLE_NAME not in tables:
        print(f"Creating table: {HBASE_TABLE_NAME}")
        # Create a table with one column family
        connection.create_table(
            HBASE_TABLE_NAME,
            {HBASE_COLUMN_FAMILY: dict()}  # Default settings for column family
        )
        print(f"Table {HBASE_TABLE_NAME} created successfully")
    else:
        print(f"Table {HBASE_TABLE_NAME} already exists")

def store_comment(connection, row_key, data):
    """Store a comment in HBase table.

    Args:
        connection: HBase connection
        row_key: Unique identifier for the row
        data: Dict containing comment data
    """
    table = connection.table(HBASE_TABLE_NAME)

    # Convert all values to bytes for HBase storage
    mapped_data = {
        f"{HBASE_COLUMN_FAMILY}:timestamp": str(int(time.time())).encode('utf-8'),
        f"{HBASE_COLUMN_FAMILY}:user_id": data['user_id'].encode('utf-8'),
        f"{HBASE_COLUMN_FAMILY}:comment": data['comment'].encode('utf-8'),
        f"{HBASE_COLUMN_FAMILY}:processed": b"False"
    }

    # Store in HBase
    table.put(row_key.encode('utf-8'), mapped_data)

def update_comment_with_sentiment(connection, row_key, sentiment):
    """Update a comment in HBase with sentiment analysis results.

    Args:
        connection: HBase connection
        row_key: Unique identifier for the row
        sentiment: String containing the sentiment label
    """
    table = connection.table(HBASE_TABLE_NAME)

    # Convert values to bytes for HBase storage
    mapped_data = {
        f"{HBASE_COLUMN_FAMILY}:sentiment": sentiment.encode('utf-8'),
        f"{HBASE_COLUMN_FAMILY}:processed": b"True",
        f"{HBASE_COLUMN_FAMILY}:processed_timestamp": str(int(time.time())).encode('utf-8')
    }

    # Update in HBase
    table.put(row_key.encode('utf-8'), mapped_data)

def get_comment(connection, row_key):
    """Retrieve a comment from HBase table.

    Args:
        connection: HBase connection
        row_key: Unique identifier for the row

    Returns:
        dict: Dictionary containing the comment data
    """
    table = connection.table(HBASE_TABLE_NAME)

    # Get the row data
    row = table.row(row_key.encode('utf-8'))

    # Convert bytes back to strings
    result = {}
    for key, value in row.items():
        column_name = key.decode('utf-8').split(':')[1]
        result[column_name] = value.decode('utf-8')

    return result