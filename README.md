# Kafka Python Project

A minimal, production-ready Kafka setup with Python producers and consumers.

## Project Structure
```
.
├── docker-compose.yml    # Kafka and Zookeeper services
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── src/
    ├── config.py       # Configuration module
    ├── producer.py     # Kafka producer
    └── consumer.py     # Kafka consumer
```

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- pip (Python package manager)

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

3. Start Kafka and Zookeeper:
   ```bash
   docker-compose up -d
   ```

4. Wait about 30 seconds for Kafka to be fully ready to accept connections.

## Usage

1. Start the consumer in one terminal:
   ```bash
   python src/consumer.py
   ```

2. Start the producer in another terminal:
   ```bash
   python src/producer.py
   ```

3. You should see messages being produced and consumed.

## Stopping the Services

1. Stop the Python scripts with Ctrl+C
2. Stop Kafka and Zookeeper:
   ```bash
   docker-compose down
   ```

## Configuration

The following environment variables can be configured in `.env`:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `KAFKA_TOPIC`: Topic name for messages

## Notes

- The Kafka setup uses the latest stable version (Confluent Platform 7.4.0)
- Images are configured for Mac M1 compatibility
- Default topic is "example-topic"
- Consumer group ID is "example-consumer-group"