# Sentiment Analysis Big Data Simulation

A real-time sentiment analysis pipeline using Kafka and HBase, optimized for Mac M1/Apple Silicon.

## Project Structure
```
.
├── docker-compose.yml    # Kafka, Zookeeper, and HBase services
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── src/
    ├── config.py       # Configuration module
    ├── producer.py     # Kafka producer for comment simulation
    ├── consumer.py     # Kafka consumer that stores in HBase
    └── hbase_utils.py  # HBase utility functions
```

## Features

- Real-time comment streaming simulation using Kafka
- HBase storage for raw comments with configurable schema
- M1/ARM64 compatible Docker setup
- Configurable environment for local development

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- pip (Python package manager)
- Mac with M1/Apple Silicon chip (or equivalent ARM64 architecture)

## Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Kafka, Zookeeper, and HBase:
   ```bash
   docker-compose up -d
   ```

4. Wait about 30-60 seconds for the services to be fully ready.

## Usage

1. Start the consumer in one terminal:
   ```bash
   python src/consumer.py
   ```

2. Start the producer in another terminal:
   ```bash
   python src/producer.py
   ```

3. You should see comments being produced, consumed, and stored in HBase.

## HBase Schema

The system stores raw comments in an HBase table with the following structure:

- Table: `comments` (configurable)
- Column Family: `data`
- Columns:
  - `data:timestamp`: Unix timestamp when the comment was processed
  - `data:user_id`: Unique identifier of the user
  - `data:comment`: Raw comment text

## Exploring HBase Data

You can access the HBase Web UI at http://localhost:16010 once the containers are running.

To interact with HBase data using Python:

```python
from src.hbase_utils import get_connection

# Connect to HBase
conn = get_connection()

# Get a reference to the comments table
table = conn.table('comments')

# Scan the first few rows
for key, data in table.scan(limit=5):
    print(f"Row key: {key.decode('utf-8')}")
    for column, value in data.items():
        print(f"  {column.decode('utf-8')}: {value.decode('utf-8')}")
```

## Stopping the Services

1. Stop the Python scripts with Ctrl+C
2. Stop all Docker containers:
   ```bash
   docker-compose down
   ```

## Configuration

The following environment variables can be configured in `.env`:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `KAFKA_TOPIC`: Topic name for messages
- `HBASE_HOST`: HBase host address
- `HBASE_PORT`: HBase Thrift API port
- `HBASE_TABLE_NAME`: Table to store comments
- `HBASE_COLUMN_FAMILY`: Column family for comments data