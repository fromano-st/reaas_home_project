# Real-Time Streaming ETL Pipeline Runbook

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Component Details](#component-details)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Operations](#operations)
- [Monitoring & Observability](#monitoring--observability)
- [Data Schema](#data-schema)
- [Troubleshooting](#troubleshooting)
- [Maintenance](#maintenance)
- [Security Considerations](#security-considerations)

## Overview

This runbook provides comprehensive documentation for a production-grade real-time streaming ETL pipeline designed to process IoT sensor data. The system implements a complete data processing workflow from ingestion through storage, with real-time monitoring and analytics capabilities.

### Key Features
- **Real-time Data Processing**: Sub-second latency for IoT event processing
- **Scalable Architecture**: Horizontally scalable components using containerization
- **Fault Tolerance**: At-least-once delivery guarantees with error handling
- **Observability**: Comprehensive monitoring with Prometheus and Grafana
- **Data Quality**: Schema validation and quality checks throughout the pipeline
- **Storage Optimization**: Parquet format with partitioning for efficient querying

### Technology Stack
- **Message Broker**: Apache Kafka with Zookeeper
- **Stream Processing**: Python-based Kafka consumers with windowed aggregations
- **Batch Processing**: Apache Spark for complex analytics
- **Storage**: MinIO (S3-compatible) object storage
- **Monitoring**: Prometheus, Grafana, Kafka UI
- **Containerization**: Docker and Docker Compose
- **Infrastructure**: Terraform for infrastructure as code

## System Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   IoT Devices   │───▶│  Kafka Broker   │───▶│ Stream Processor│
│   (Simulated)   │    │   + Zookeeper   │    │  (Python ETL)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Kafka UI       │    │   Monitoring    │    │   MinIO S3      │
│  (Management)   │    │ Prometheus +    │    │   (Storage)     │
└─────────────────┘    │   Grafana       │    └─────────────────┘
                       └─────────────────┘              │
                                                        ▼
                                              ┌─────────────────┐
                                              │  Spark ETL      │
                                              │ (Analytics)     │
                                              └─────────────────┘
```

### Component Architecture

#### 1. Data Ingestion Layer
- **IoT Producer** (`producer.py`):
  - Simulates real IoT devices generating sensor data
  - Multi-threaded architecture with health check endpoints
  - Supports 10 device types with realistic data patterns
  - Configurable event rates and device counts
- **Apache Kafka**:
  - High-throughput message broker for event streaming
  - Single broker setup with auto-topic creation
  - JMX metrics enabled for monitoring
- **Zookeeper**:
  - Coordination service for Kafka cluster management
  - Persistent data storage for cluster state

#### 2. Stream Processing Layer
- **Python Kafka Consumer** (`kafka_aggregator.py`):
  - Real-time ETL processing with windowed aggregations
  - Pydantic-based schema validation and quality checks
  - Thread-safe aggregation with configurable batch sizes
  - Automatic retry logic and graceful error handling
- **Spark Streaming**:
  - Alternative processing engine for complex transformations
  - Structured streaming with watermarking support
- **Data Validation**:
  - Schema enforcement using Pydantic models
  - Quality flags for battery level and signal strength
  - Invalid data logging and discarding

#### 3. Storage Layer
- **MinIO**:
  - S3-compatible object storage for raw and processed data
  - Health checks and automatic bucket creation
  - Configurable retention policies
- **Parquet Format**:
  - Columnar storage format optimized for analytics
  - Snappy compression for space efficiency
  - Schema evolution support
- **Partitioning**:
  - Time-based partitioning (year/month/day/hour) for efficient querying
  - Separate paths for raw and aggregated data

#### 4. Analytics Layer
- **Spark ETL** (`spark_s3_percentile_etl.py`):
  - Batch processing for complex analytics and percentile calculations
  - Statistical outlier detection using 3-sigma rule
  - Memory-optimized operations with configurable parallelism
- **Query Engine**:
  - SQL-based analytics with outlier detection and filtering
  - Support for complex aggregations and window functions
  - CSV output for result validation

#### 5. Monitoring Layer
- **Prometheus**:
  - Metrics collection and storage with configurable retention
  - Custom metrics from application components
- **Grafana**:
  - Real-time dashboards and alerting
  - Pre-configured dashboards for pipeline monitoring
- **JMX Exporter**:
  - Kafka metrics exposure via HTTP endpoint
  - Custom JMX configuration for relevant metrics
- **Kafka UI**:
  - Web-based Kafka cluster management and monitoring
  - Topic, consumer group, and message inspection

## Component Details

### IoT Event Producer (`producer.py`)

The IoT producer simulates real-world IoT devices by generating synthetic sensor data with realistic patterns and variations.

**Key Features:**
- Supports 10 different device types (temperature, humidity, pressure, etc.)
- Configurable event generation rates and patterns
- Built-in health check endpoint for monitoring
- Realistic data generation with proper units and ranges

**Device Types Supported:**
- Temperature Sensors (°C)
- Humidity Sensors (%)
- Pressure Sensors (hPa)
- Light Sensors (lux)
- Motion Detectors (boolean)
- Door/Window Sensors (boolean)
- Smoke Detectors (boolean)
- CO2 Sensors (ppm)
- Air Quality Sensors (0-500 index)

### Kafka Consumer ETL (`kafka_aggregator.py`)

The Python-based stream processor consumes events from Kafka and performs real-time ETL operations.

**Processing Features:**
- **Schema Validation**: Pydantic-based validation with error handling
- **Quality Checks**: Battery level and signal strength validation
- **Time Windowing**: 1-minute windows with 2-minute watermark for late data
- **Aggregations**: Statistical computations per device per window
- **Fault Tolerance**: Automatic retry logic and error recovery

**Aggregation Metrics:**
- Average, min, max sensor values
- Standard deviation of readings
- Event counts and durations
- Battery and signal strength averages
- Quality flag counts (low battery, weak signal)

### Spark Analytics Engine (`spark_s3_percentile_etl.py`)

Advanced analytics engine for complex queries and statistical computations.

**Capabilities:**
- **Outlier Detection**: 3-sigma rule for statistical outlier removal
- **Percentile Calculations**: 95th percentile computations with statistical rigor
- **Data Filtering**: Minimum event count thresholds (500+ events per day)
- **Performance Optimization**: Memory tuning and shuffle optimization

## Data Flow

### Real-Time Processing Flow

1. **Data Generation**: IoT producer generates events every 1-5 seconds
2. **Message Queuing**: Events published to Kafka topic `iot-events`
3. **Stream Processing**: Python consumer processes events in 1-minute windows
4. **Data Validation**: Schema validation and quality checks applied
5. **Aggregation**: Statistical computations performed per device
6. **Storage**: Raw and aggregated data written to MinIO in Parquet format
7. **Monitoring**: Metrics collected and visualized in real-time

### Batch Processing Flow

1. **Data Reading**: Spark reads Parquet files from MinIO
2. **Outlier Detection**: Statistical outlier removal using 3-sigma rule
3. **Filtering**: Device types with <500 events filtered out
4. **Percentile Calculation**: 95th percentile computed per device type per day
5. **Results Storage**: Output saved as CSV for validation

### Data Partitioning Strategy

```
s3://bucket/
├── raw/
│   └── year=2025/month=07/day=28/hour=14/
│       └── events_1722175234.parquet
└── aggregated/
    └── year=2025/month=07/day=28/hour=14/
        └── agg_1722175234.parquet
```

## Prerequisites

### System Requirements
- **Operating System**: Unix-based environment (Linux, macOS)
- **Memory**: Minimum 8GB RAM (16GB recommended)
- **Storage**: 10GB free disk space
- **Network**: Internet connectivity for Docker image downloads

### Software Dependencies
- **Docker**: Version 20.0+ with Docker Compose
- **Python**: Version 3.11+ for development and testing
- **Make**: For automation scripts
- **Git**: For version control
- **AWS CLI**: (Optional) For cloud deployment

### Development Tools
- **Terraform**: Version 1.10+ for infrastructure provisioning
- **pytest**: For running unit and integration tests

## Installation & Setup

### 1. Repository Setup

```bash
# Clone the repository
git clone <repository-url>
cd <project-directory>

# Copy environment configuration
cp _env_sample.txt .env
```

### 2. Environment Configuration

Edit the `.env` file with your specific configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=iot-events
KAFKA_GROUP_ID=iot-consumer-group

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
AWS_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
AWS_S3_BUCKET_DATA=iot-data

# Monitoring Configuration
GRAFANA_ADMIN_PASSWORD=admin123

# Producer Configuration
PRODUCER_EVENTS_PER_SECOND=2
PRODUCER_DEVICE_COUNT=50
```

### 3. Infrastructure Deployment



#### Option A: Step-by-Step Deployment
```bash
# 1. Start infrastructure services
make setup-infrastructure

# 2. Start data pipeline
make start-producer
docker-compose up -d python-etl-stream

# 3. Generate historical data (optional)
make generate-historical-data
```
#### Option B: Quick Start
```bash
# Start all services with one command
make start-all
```

#### Option C: Terraform Deployment
```bash
# Initialize and apply Terraform
make terraform-init
make terraform-plan
make terraform-apply
```

### 4. Service Verification

```bash
# Check service status
make status

# View logs
make logs-all

# Test connectivity
make test-s3
```

## Configuration

### Kafka Configuration

**Topic Configuration:**
- **Topic Name**: `iot-events`
- **Partitions**: 3 (configurable)
- **Replication Factor**: 1 (single broker setup)
- **Retention**: 7 days

**Consumer Configuration:**
- **Group ID**: `iot-consumer-group`
- **Auto Offset Reset**: `earliest`
- **Enable Auto Commit**: `true`
- **Session Timeout**: 30 seconds

### Stream Processing Configuration

**Window Configuration:**
- **Window Size**: 1 minute
- **Watermark**: 2 minutes (late data tolerance)
- **Trigger**: Processing time trigger

**Aggregation Configuration:**
- **Batch Size**: 100 events
- **Flush Interval**: 30 seconds
- **Quality Thresholds**:
  - Battery Level: ≥20%
  - Signal Strength: ≥-90 dBm

### Storage Configuration

**MinIO Configuration:**
- **Endpoint**: `http://minio:9000`
- **Bucket**: `iot-data`
- **Format**: Parquet with Snappy compression
- **Partitioning**: Time-based (year/month/day/hour)

**Spark Configuration:**
- **Master**: `local[*]` (all available cores)
- **Memory**: 2GB driver, 1GB executor
- **Serializer**: Kryo serializer for performance

## Operations

### Starting the Pipeline

#### Complete Pipeline Startup
```bash
# Start all services
make start-all

# Verify services are running
docker-compose ps

# Check logs for any errors
make logs-all
```

#### Individual Service Management
```bash
# Start producer only
make start-producer

# Start streaming consumer
docker-compose up -d python-etl-stream

# Start Spark ETL job
docker-compose up spark-etl
```

### Stopping the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (data loss)
docker-compose down -v

# Clean all data
make clean-all
```

### Data Pipeline Operations

#### Monitoring Data Flow
```bash
# Check Kafka topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Monitor consumer lag
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group iot-consumer-group

# Check MinIO buckets
docker-compose exec minio mc ls minio/iot-data/
```

#### Running Analytics Queries
```bash
# Execute Spark percentile query
docker-compose up spark-etl

# View results
docker-compose exec spark-etl cat /opt/spark/work-dir/output/percentile_results.csv
```

### Scaling Operations

#### Horizontal Scaling
```bash
# Scale producer instances
docker-compose up -d --scale iot-producer=3

# Scale consumer instances
docker-compose up -d --scale python-etl-stream=2
```

#### Vertical Scaling
Modify `docker-compose.yml` to adjust resource limits:
```yaml
services:
  python-etl-stream:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

## Monitoring & Observability

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana Dashboard | http://localhost:3000 | admin/admin123 |
| Kafka UI | http://localhost:8080 | None |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin123 |
| Prometheus | http://localhost:9090 | None |

### Key Metrics to Monitor

#### System Health Metrics
- **Service Availability**: All containers running and healthy
- **Resource Usage**: CPU, memory, disk utilization
- **Network Connectivity**: Inter-service communication

#### Kafka Metrics
- **Message Throughput**: Messages per second produced/consumed
- **Consumer Lag**: Delay between production and consumption
- **Partition Distribution**: Even distribution across partitions
- **Error Rates**: Failed message processing rates



## Data Schema

### IoT Event Schema (Raw Data)

```json
{
  "event_duration": 1.5,
  "device_type": "TEMPERATURE_SENSOR",
  "device_id": "temp_001_living_room",
  "timestamp": "2025-01-15T10:30:00Z",
  "location": "Living Room",
  "value": 23.5,
  "unit": "°C",
  "battery_level": 85,
  "signal_strength": -65,
  "metadata": {
    "firmware_version": "1.2.3",
    "calibration_date": "2024-12-01"
  }
}
```

### Processed Data Schema (Aggregated)

```json
{
  "window_start": "2025-01-15T10:30:00Z",
  "window_end": "2025-01-15T10:31:00Z",
  "device_id": "temp_001_living_room",
  "device_type": "TEMPERATURE_SENSOR",
  "location": "Living Room",
  "unit": "°C",
  "avg_value": 23.2,
  "event_count": 12,
  "min_value": 22.8,
  "max_value": 23.7,
  "stddev_value": 0.3,
  "avg_event_duration": 1.4,
  "avg_battery_level": 84.5,
  "avg_signal_strength": -66.2,
  "low_battery_count": 0,
  "weak_signal_count": 1,
  "year": 2025,
  "month": 1,
  "day": 15,
  "hour": 10
}
```

### Data Quality Flags

- **is_valid_battery**: Battery level ≥ 20%
- **is_valid_signal**: Signal strength ≥ -90 dBm
- **processing_timestamp**: When the event was processed
- **partition_fields**: year, month, day, hour for efficient querying

## Testing
### Testing Strategy
The pipeline implements a comprehensive testing strategy covering unit tests, integration tests, and end-to-end validation to ensure data quality and system reliability.

#### Unit Tests
unit tests can be executed with pytest
```
pip install -r tests/requirements.txt
pytest -v
```
#### Integration Tests
 build the infrastructure:
`make setup-infrastructure`
then `test_s3` (test operation on MinIO storage) can be executed:
`make test-s3`
starting the producer is necessary for other tests:
`make start-producer`
it can be tested with:
`docker-compose up kafka-consumer-tester`



### CI/CD Pipeline
The project includes GitHub Actions workflows:
**Feature Branch CI:** Linting, formatting , Docker builds
**Main Branch CI:** Full testing,Docker builds, integration tests
**CD Pipeline:** Automated Terraform deployment

## Troubleshooting

### Common Issues and Solutions

#### 1. Services Not Starting

**Symptoms:**
- Docker containers failing to start
- Port conflicts
- Resource allocation errors

**Diagnosis:**
```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs <service-name>

# Check resource usage
docker stats
```

**Solutions:**
```bash
# Free up ports
sudo lsof -i :9092  # Check Kafka port
sudo kill -9 <PID>  # Kill conflicting process

# Increase Docker resources
# Docker Desktop: Settings > Resources > Advanced

# Clean up Docker system
docker system prune -a
```

#### 2. Kafka Connection Issues

**Symptoms:**
- Producer cannot connect to Kafka
- Consumer lag increasing
- Message delivery failures

**Diagnosis:**
```bash
# Test Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check Kafka logs
docker-compose logs kafka

# Monitor consumer groups
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

**Solutions:**
```bash
# Restart Kafka services
docker-compose restart zookeeper kafka

# Reset consumer group (data loss)
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group iot-consumer-group --reset-offsets --to-earliest --all-topics --execute

# Check network connectivity
docker-compose exec iot-producer ping kafka
```

#### 3. MinIO Storage Issues

**Symptoms:**
- Cannot write to MinIO
- Authentication failures
- Bucket access errors

**Diagnosis:**
```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# Test S3 operations
make test-s3

# Check MinIO logs
docker-compose logs minio
```

**Solutions:**
```bash
# Restart MinIO
docker-compose restart minio

# Recreate buckets
make setup-infrastructure

# Check credentials in .env file
cat .env | grep MINIO
```

#### 4. Processing Pipeline Issues

**Symptoms:**
- High processing latency
- Data validation errors
- Missing aggregations

**Diagnosis:**
```bash
# Check consumer logs
docker-compose logs python-etl-stream

# Monitor processing metrics
curl http://localhost:9090/api/v1/query?query=kafka_consumer_lag_sum

# Validate data schema
docker-compose exec python-etl-stream python -c "from src.kafka_consumer_etl.schema import IoTEventValidator; print('Schema OK')"
```

**Solutions:**
```bash
# Restart processing services
docker-compose restart python-etl-stream

# Scale processing instances
docker-compose up -d --scale python-etl-stream=2

# Check data quality
docker-compose logs python-etl-stream | grep "validation error"
```

#### 5. Monitoring and Dashboards

**Symptoms:**
- Grafana dashboards not loading
- Missing metrics in Prometheus
- JMX exporter connection issues

**Diagnosis:**
```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Test Grafana connectivity
curl http://localhost:3000/api/health

# Check JMX exporter
curl http://localhost:5556/metrics
```
---

## Conclusion

This runbook provides comprehensive guidance for operating the real-time streaming ETL pipeline. For additional support or questions, refer to the project documentation.

**Last Updated**: 2025-07-28
