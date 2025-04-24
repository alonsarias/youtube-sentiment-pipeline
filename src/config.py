from dotenv import load_dotenv
import os

load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

# HBase configuration
HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', 9090))
HBASE_TABLE_NAME = os.getenv('HBASE_TABLE_NAME', 'comments')
HBASE_COLUMN_FAMILY = os.getenv('HBASE_COLUMN_FAMILY', 'data')