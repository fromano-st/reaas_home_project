# Kafka Consumer Tester

This test validates Kafka connectivity and message consumption from the `iot-producer` service. It follows the same pattern as the `s3_tester` and is designed to verify that:

1. Kafka connection is working properly
2. Messages are being produced to the `iot-events` topic
3. Messages conform to the expected IoT event schema
4. Consumer can successfully read and validate messages

## Features

- **Connection Testing**: Verifies basic Kafka connectivity and cluster metadata access
- **Schema Validation**: Validates consumed messages against the expected IoT event schema using Pydantic
- **Statistics Reporting**: Provides detailed statistics about consumption success rates
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals for clean shutdown
- **Configurable Limits**: Supports configurable message count and timeout limits
- **Comprehensive Logging**: Detailed logging of all operations and message validation

## Configuration

The tester uses the following environment variables:

### Required Variables (from .env)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: `kafka:29092`)
- `KAFKA_TOPIC`: Kafka topic to consume from (default: `iot-events`)

### Optional Test Configuration
- `KAFKA_TEST_MAX_MESSAGES`: Maximum number of messages to consume (default: `10`)
- `KAFKA_TEST_TIMEOUT`: Timeout in seconds for message consumption (default: `30`)

## Usage

### Using Docker Compose (Recommended)

```bash
# Run the Kafka consumer test
docker-compose up kafka-consumer-tester

# Run with custom configuration
KAFKA_TEST_MAX_MESSAGES=20 KAFKA_TEST_TIMEOUT=60 docker-compose up kafka-consumer-tester
```

### Standalone Docker

```bash
# Build the image
docker build -t kafka-consumer-test ./tests/kafka_consumer_tester

# Run the test
docker run --rm --env-file .env kafka-consumer-test
```

### Direct Python Execution

```bash
cd tests/kafka_consumer_tester
pip install -r requirements.txt
python test_kafka_consumer.py
```

## Expected Output

The test will output detailed logs showing:

1. **Connection Test**: Verification of Kafka connectivity
2. **Consumer Creation**: Setup of the Kafka consumer
3. **Message Consumption**: Real-time logging of consumed messages with validation
4. **Statistics Summary**: Final report with consumption metrics

### Example Success Output

```
2025-07-28 14:17:34,123 - __main__ - INFO - Starting comprehensive Kafka consumer tests...
2025-07-28 14:17:34,124 - __main__ - INFO - Testing Kafka connection...
2025-07-28 14:17:34,234 - __main__ - INFO - Connection successful - able to retrieve cluster metadata
2025-07-28 14:17:34,235 - __main__ - INFO - Topic 'iot-events' found in cluster
2025-07-28 14:17:34,236 - __main__ - INFO - Creating Kafka consumer...
2025-07-28 14:17:34,345 - __main__ - INFO - Kafka consumer created successfully
2025-07-28 14:17:34,346 - __main__ - INFO - Starting to consume messages from topic 'iot-events'...

--- Message 1 ---
2025-07-28 14:17:35,123 - __main__ - INFO - Partition: 0, Offset: 1234
2025-07-28 14:17:35,124 - __main__ - INFO - Key: temperature_sensor_001_living_room
2025-07-28 14:17:35,125 - __main__ - INFO - ✓ Valid message from device temperature_sensor_001_living_room (temperature_sensor) at Living Room
2025-07-28 14:17:35,126 - __main__ - INFO -   Value: 22.5 °C, Duration: 2.3s

==================================================
KAFKA CONSUMER TEST STATISTICS
==================================================
Total messages consumed: 10
Valid messages: 10
Invalid messages: 0
Success rate: 100.0%
Device types seen: ['door_sensor', 'humidity_sensor', 'temperature_sensor']
Locations seen: ['Kitchen', 'Living Room', 'Office']
==================================================
2025-07-28 14:17:45,789 - __main__ - INFO - ✓ Kafka consumer tests passed successfully!
```

## Message Schema Validation

The tester validates each consumed message against the expected IoT event schema:

```python
{
    "event_duration": float,      # Required: 0.1 <= value <= 3600.0
    "device_type": str,          # Required: device type identifier
    "device_id": str,            # Required: unique device identifier
    "timestamp": str,            # Required: ISO format timestamp
    "location": str,             # Required: device location
    "value": float,              # Required: sensor measurement
    "unit": str,                 # Required: unit of measurement
    "battery_level": int,        # Optional: 0-100
    "signal_strength": int,      # Optional: -120 to -30 dBm
    "metadata": dict             # Optional: additional metadata
}
```

## Integration with Docker Compose

The service is configured in `docker-compose.yml` with:

- **Network**: Connected to `etl-network`
- **Dependencies**: Waits for `kafka` and `iot-producer` services
- **Environment**: Inherits from `.env` file
- **Configuration**: Custom test limits via environment variables

## Troubleshooting

### No Messages Consumed
- Verify the `iot-producer` service is running and producing messages
- Check that the topic name matches between producer and consumer
- Ensure Kafka is properly started and accessible

### Schema Validation Failures
- Check that the producer is sending messages in the expected format
- Verify the IoT event schema hasn't changed
- Review the detailed error logs for specific validation issues

### Connection Issues
- Verify `KAFKA_BOOTSTRAP_SERVERS` points to the correct Kafka instance
- Ensure the consumer can reach Kafka (network connectivity)
- Check Kafka service health and logs

## Dependencies

- `kafka-python==2.0.2`: Kafka client library
- `python-dotenv==1.0.0`: Environment variable loading
- `pydantic==2.5.0`: Data validation and schema enforcement