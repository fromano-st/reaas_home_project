# Kafka Aggregator with Parquet Output

This service consumes IoT events from Kafka, performs real-time aggregations similar to `streaming_etl.py`, and writes both raw data and aggregated results to MinIO/S3 in Parquet format. It provides a pure Python alternative to the Spark-based streaming ETL pipeline.

## Features

- **Real-time Stream Processing**: Consumes messages from Kafka with configurable batching
- **Data Quality Validation**: Applies the same validation rules as streaming_etl.py
- **Time-based Windowing**: Creates 1-minute aggregation windows with watermark handling
- **Statistical Aggregations**: Computes avg, min, max, count, stddev for each window
- **Parquet Output**: Writes data in efficient Parquet format with partitioning
- **MinIO/S3 Integration**: Uses boto3 for reliable cloud storage integration
- **Graceful Shutdown**: Handles signals and flushes data before termination
- **Comprehensive Logging**: Detailed logging of all processing steps

## Architecture

```
Kafka Topic (iot-events)
    ↓
Message Validation & Quality Checks
    ↓
Time Window Management (1-minute windows)
    ↓
Statistical Aggregations
    ↓
Parquet File Generation
    ↓
MinIO/S3 Storage (Partitioned by year/month/day/hour)
```

## Data Processing Pipeline

### 1. Message Validation
- Schema validation using Pydantic models
- Data quality checks (ranges, null values, etc.)
- Invalid messages are logged and discarded

### 2. Quality Flags
- `is_valid_battery`: Battery level >= 20%
- `is_valid_signal`: Signal strength >= -90 dBm

### 3. Time Windowing
- **Window Size**: 1 minute (configurable)
- **Watermark**: 2 minutes (late data handling)
- **Partitioning**: By device_id for parallel processing

### 4. Aggregations (per window per device)
- `avg_value`: Average sensor reading
- `event_count`: Number of events in window
- `min_value` / `max_value`: Min/max sensor readings
- `stddev_value`: Standard deviation of readings
- `avg_event_duration`: Average event duration
- `avg_battery_level`: Average battery level
- `avg_signal_strength`: Average signal strength
- `low_battery_count`: Count of low battery events
- `weak_signal_count`: Count of weak signal events

## Configuration

### Required Environment Variables (from .env)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers
- `KAFKA_TOPIC`: Kafka topic to consume from
- `AWS_ENDPOINT`: MinIO/S3 endpoint URL
- `AWS_ACCESS_KEY_ID`: S3 access key
- `AWS_SECRET_ACCESS_KEY`: S3 secret key
- `AWS_S3_BUCKET_DATA`: S3 bucket for data storage

### Optional Configuration
- `AGGREGATOR_BATCH_SIZE`: Batch size for processing (default: 100)
- `AGGREGATOR_FLUSH_INTERVAL`: Flush interval in seconds (default: 30)

## Output Structure

### Raw Data Path
```
s3://bucket/raw/year=2025/month=07/day=28/hour=14/events_1722175234.parquet
```

### Aggregated Data Path
```
s3://bucket/aggregated/year=2025/month=07/day=28/hour=14/agg_1722175234.parquet
```

### Raw Data Schema
```python
{
    "event_duration": float,
    "device_type": str,
    "device_id": str,
    "timestamp": str,  # ISO format
    "location": str,
    "value": float,
    "unit": str,
    "battery_level": int,
    "signal_strength": int,
    "metadata": dict,
    "is_valid_battery": bool,
    "is_valid_signal": bool,
    "processing_timestamp": str,  # ISO format
    "year": int,
    "month": int,
    "day": int,
    "hour": int
}
```

### Aggregated Data Schema
```python
{
    "window_start": str,  # ISO format
    "window_end": str,    # ISO format
    "device_id": str,
    "device_type": str,
    "location": str,
    "unit": str,
    "avg_value": float,
    "event_count": int,
    "min_value": float,
    "max_value": float,
    "stddev_value": float,
    "avg_event_duration": float,
    "avg_battery_level": float,
    "avg_signal_strength": float,
    "low_battery_count": int,
    "weak_signal_count": int,
    "year": int,
    "month": int,
    "day": int,
    "hour": int
}
```

## Usage

### Using Docker Compose (Recommended)

```bash
# Run the Kafka aggregator
docker-compose up kafka-aggregator-tester

# Run with custom configuration
AGGREGATOR_BATCH_SIZE=200 AGGREGATOR_FLUSH_INTERVAL=60 docker-compose up kafka-aggregator-tester

# Run in background
docker-compose up -d kafka-aggregator-tester
```

### Standalone Docker

```bash
# Build the image
docker build -t kafka-aggregator ./tests/kafka_aggregator_tester

# Run the aggregator
docker run --rm --env-file .env kafka-aggregator
```

### Direct Python Execution

```bash
cd tests/kafka_aggregator_tester
pip install -r requirements.txt
python kafka_aggregator.py
```

## Expected Output

### Startup Logs
```
2025-07-28 14:17:34,123 - __main__ - INFO - Starting Kafka Aggregator with Parquet output...
2025-07-28 14:17:34,124 - __main__ - INFO - Initialized Kafka Aggregator
2025-07-28 14:17:34,125 - __main__ - INFO - Kafka servers: kafka:29092
2025-07-28 14:17:34,126 - __main__ - INFO - Topic: iot-events
2025-07-28 14:17:34,127 - __main__ - INFO - Batch size: 100
2025-07-28 14:17:34,128 - __main__ - INFO - Flush interval: 30s
```

### Processing Logs
```
2025-07-28 14:17:35,123 - __main__ - INFO - Processed 100 events
2025-07-28 14:17:36,234 - __main__ - INFO - Flushing 100 events to storage...
2025-07-28 14:17:36,345 - __main__ - INFO - ✓ Wrote 100 raw events to s3://bucket/raw/year=2025/month=07/day=28/hour=14/events_1722175056.parquet
2025-07-28 14:17:37,456 - __main__ - INFO - Processing 5 completed window aggregations...
2025-07-28 14:17:37,567 - __main__ - INFO - ✓ Wrote 5 aggregations to s3://bucket/aggregated/year=2025/month=07/day=28/hour=14/agg_1722175057.parquet
```

### Statistics Summary
```
============================================================
KAFKA AGGREGATOR STATISTICS
============================================================
Events processed: 1500
Raw events written: 1500
Aggregations written: 75
Active windows: 12
============================================================
```

## Comparison with streaming_etl.py

| Feature | streaming_etl.py (Spark) | kafka_aggregator.py (Python) |
|---------|---------------------------|-------------------------------|
| **Processing Engine** | Spark Structured Streaming | Pure Python with pandas |
| **Windowing** | Built-in window functions | Custom TimeWindowManager |
| **Aggregations** | Spark SQL functions | NumPy/pandas calculations |
| **Storage** | Direct Parquet write | PyArrow + boto3 |
| **Partitioning** | Automatic | Manual implementation |
| **Scalability** | Horizontal (Spark cluster) | Vertical (single process) |
| **Resource Usage** | High (JVM + Spark) | Low (Python process) |
| **Latency** | Lower (streaming) | Slightly higher (batching) |
| **Complexity** | Higher (Spark setup) | Lower (pure Python) |

## Performance Considerations

### Throughput
- **Expected**: 1000-5000 events/second (single process)
- **Bottlenecks**: Network I/O to MinIO, Parquet serialization
- **Optimization**: Increase batch size, reduce flush interval

### Memory Usage
- **Raw Events Buffer**: ~100MB for 10k events
- **Window Storage**: ~50MB for 1000 active windows
- **Parquet Buffers**: ~10MB per file

### Storage Efficiency
- **Parquet Compression**: ~70% size reduction vs JSON
- **Partitioning**: Enables efficient querying by time
- **Columnar Format**: Optimized for analytics workloads

## Monitoring and Troubleshooting

### Key Metrics to Monitor
- Events processed per second
- Window completion rate
- S3 write success rate
- Memory usage and buffer sizes

### Common Issues

#### High Memory Usage
- Reduce `AGGREGATOR_BATCH_SIZE`
- Decrease `AGGREGATOR_FLUSH_INTERVAL`
- Check for window accumulation

#### S3 Write Failures
- Verify MinIO connectivity and credentials
- Check bucket permissions
- Monitor network connectivity

#### Missing Aggregations
- Verify watermark settings
- Check event timestamp validity
- Monitor window completion logs

## Integration with Analytics

The generated Parquet files can be queried using:

- **Apache Spark**: For large-scale analytics
- **Pandas**: For local analysis
- **DuckDB**: For SQL queries on Parquet files
- **Apache Drill**: For distributed querying
- **Presto/Trino**: For federated queries

### Example Query (DuckDB)
```sql
SELECT 
    device_type,
    location,
    AVG(avg_value) as daily_avg,
    COUNT(*) as window_count
FROM 's3://bucket/aggregated/year=2025/month=07/day=28/**/*.parquet'
WHERE device_type = 'temperature_sensor'
GROUP BY device_type, location
ORDER BY daily_avg DESC;
```

## Dependencies

- `kafka-python==2.0.2`: Kafka client library
- `python-dotenv==1.0.0`: Environment variable loading
- `pydantic==2.5.0`: Data validation and schema enforcement
- `pandas==2.1.4`: Data manipulation and analysis
- `pyarrow==14.0.2`: Parquet file format support
- `boto3==1.34.0`: AWS/MinIO S3 client
- `numpy==1.26.2`: Numerical computations