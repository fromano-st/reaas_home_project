# Spark Streaming Kafka Consumer Tester

This test suite validates Spark Streaming connectivity to Kafka and consumes IoT events from the `iot-producer` service with configurable timeouts.

## Overview

The Spark Streaming Tester is designed to:
- Test Kafka connectivity using Spark Structured Streaming
- Consume messages from the IoT producer
- Log consumed messages to stdout
- Implement timeout functionality for both producer and consumer
- Provide comprehensive test results

## Features

- **Kafka Connectivity Test**: Validates connection to Kafka cluster
- **Message Consumption**: Consumes and displays IoT events in real-time
- **Timeout Management**: Configurable timeouts for producer readiness and consumer operation
- **Graceful Shutdown**: Handles interruption signals properly
- **Comprehensive Logging**: Detailed logging to stdout for monitoring
- **Schema Validation**: Validates IoT event message structure

## Configuration

The tester uses environment variables for configuration:

### Required Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (e.g., `kafka:29092`)
- `KAFKA_TOPIC`: Kafka topic to consume from (e.g., `iot-events`)

### Optional Variables (with defaults)
- `CONSUMER_TIMEOUT`: Consumer operation timeout in seconds (default: 60)
- `PRODUCER_TIMEOUT`: Producer readiness timeout in seconds (default: 30)
- `MAX_MESSAGES`: Maximum messages to consume (default: 10)
- `TEST_DURATION`: Test duration in seconds (default: 30)

## Usage

### Using Docker Compose

The tester is integrated into the docker-compose setup:

```bash
# Run the complete stack including the tester
docker-compose up

# Run only the tester (after other services are up)
docker-compose up spark-streaming-tester

# View tester logs
docker-compose logs -f spark-streaming-tester
```

### Standalone Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run configuration test
python test_config.py

# Run the full streaming test
python test_spark_streaming.py
```

## Test Flow

1. **Initialization**: Load configuration and create Spark session
2. **Producer Wait**: Wait for IoT producer to be ready and sending messages
3. **Connectivity Test**: Validate Kafka connection using Spark
4. **Message Consumption**: Start streaming query to consume and display messages
5. **Timeout Management**: Monitor test duration and handle timeouts
6. **Graceful Shutdown**: Stop streaming queries and Spark session properly

## Output Format

The tester outputs consumed messages in a structured format showing:
- Device information (ID, type, location)
- Sensor readings (value, unit, timestamp)
- Metadata (battery level, signal strength, device metadata)
- Kafka metadata (partition, offset, processing timestamp)

## Example Output

```
2025-07-28 12:30:00,123 - SparkStreamingTester - INFO - Starting comprehensive Spark streaming tests...
2025-07-28 12:30:01,456 - SparkStreamingTester - INFO - ✓ Kafka connectivity test passed
2025-07-28 12:30:02,789 - SparkStreamingTester - INFO - Starting message consumption...

+------------------+--------------------+--------------------+--------------------+
|      device_id   |     device_type    |      location      |       value        |
+------------------+--------------------+--------------------+--------------------+
|temperature_001_l |TEMPERATURE_SENSOR  |    Living Room     |       23.45        |
|humidity_002_kit  |HUMIDITY_SENSOR     |      Kitchen       |       67.8         |
+------------------+--------------------+--------------------+--------------------+

2025-07-28 12:30:30,123 - SparkStreamingTester - INFO - ✅ All Spark streaming tests passed successfully!
```

## Error Handling

The tester includes comprehensive error handling for:
- Missing environment variables
- Kafka connectivity issues
- Spark session creation failures
- Streaming query errors
- Timeout scenarios
- Graceful shutdown on interruption

## Dependencies

- **PySpark 3.5.0**: Spark Structured Streaming framework
- **kafka-python 2.0.2**: Kafka client library
- **python-dotenv 1.0.0**: Environment variable management

## Integration with Docker Compose

The service is configured in `docker-compose.yml` with:
- Dependency on Kafka and IoT producer services
- Environment variable inheritance from `.env` file
- Network connectivity to the ETL network
- Restart policy set to "no" (runs once and exits)

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Kafka service is running and accessible
   - Check `KAFKA_BOOTSTRAP_SERVERS` configuration
   - Verify network connectivity

2. **Producer Not Ready**
   - Increase `PRODUCER_TIMEOUT` value
   - Check if IoT producer service is running
   - Verify producer is sending messages to the correct topic

3. **No Messages Consumed**
   - Check if producer is actively sending messages
   - Verify topic name matches between producer and consumer
   - Increase `TEST_DURATION` for longer observation

4. **Spark Session Errors**
   - Ensure sufficient memory and resources
   - Check Java installation and compatibility
   - Review Spark configuration settings

### Debugging

Enable debug logging by setting the log level:
```python
logging.basicConfig(level=logging.DEBUG)
```

View detailed Spark logs by adjusting the Spark log level:
```python
spark.sparkContext.setLogLevel("INFO")
```

## Similar to s3_tester

This implementation follows the same pattern as the `s3_tester`:
- Similar directory structure and file organization
- Consistent error handling and logging approach
- Environment variable configuration pattern
- Docker integration methodology
- Comprehensive testing approach with multiple test phases
- Graceful shutdown and cleanup procedures