Real-Time Streaming ETL Pipeline
A production-grade streaming data pipeline using Apache Kafka, a Python Kafka Consumer, and MinIO for real-time IoT data processing and analytics.

🏗️ Architecture Overview
This project implements a complete streaming ETL pipeline with the following components:

Data Ingestion: Apache Kafka with custom IoT event producer

Stream Processing: Python Kafka Consumer for real-time ETL

Storage: MinIO (S3-compatible) for processed data in Parquet format

Monitoring: Prometheus + Grafana for observability

Infrastructure: Terraform for infrastructure as code

CI/CD: GitHub Actions for automated testing and deployment

📋 Prerequisites
To execute this pipeline locally, you will need the following:

Docker and Docker Compose: For containerizing and orchestrating services.

Python 3.11+: For running the IoT event producer and the Python Kafka consumer.

Terraform 1.6+: (Optional) For provisioning infrastructure as code.

Make: For automation scripts and simplified command execution.

Git: For cloning the repository.

🚀 Quick Start
1. Clone and Setup
git clone <repository-url>
cd streaming-etl-pipeline
cp _env_sample.txt .env  # Edit with your configuration

2. Start the Infrastructure
# Start all services
make start-all

# Or start individual components
make
start-infrastructure
start-producer
start-streaming
start-python-kafka

3. Deploy Infrastructure (Optional)
# Initialize and apply Terraform
make terraform-init
make terraform-plan
make terraform-apply

4. Start Data Pipeline
# Start the producer
make producer-start

# Start the Python consumer job
make consumer-start

# Generate historical data
make historical-data

5. Access Monitoring
Grafana Dashboard: http://localhost:3000 (admin/admin123)

Kafka UI: http://localhost:8080

MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)

Prometheus: http://localhost:9090

📊 Data Schema
IoT Event Schema
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

Supported Device Types
TEMPERATURE_SENSOR: Temperature readings (°C)

HUMIDITY_SENSOR: Humidity levels (%)

PRESSURE_SENSOR: Atmospheric pressure (hPa)

LIGHT_SENSOR: Light intensity (lux)

MOTION_DETECTOR: Motion detection (boolean)

DOOR_SENSOR: Door state (boolean)

WINDOW_SENSOR: Window state (boolean)

SMOKE_DETECTOR: Smoke detection (boolean)

CO2_SENSOR: CO2 concentration (ppm)

AIR_QUALITY_SENSOR: Air quality index (0-500)

🔧 Configuration
Environment Variables
Key configuration options in .env:

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=iot-events

# Producer Configuration
PRODUCER_INTERVAL=2.0

# Python Consumer Configuration
AGGREGATOR_BATCH_SIZE=100
AGGREGATOR_FLUSH_INTERVAL=30
OUTPUT_PATH=s3a://reaas-home-project-data/processed/

# MinIO/S3 Configuration
MINIO_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123

# Monitoring
GRAFANA_ADMIN_PASSWORD=admin123

# Historical Data
HISTORICAL_DAYS=7

📈 Monitoring and Observability
Grafana Dashboards
The system includes pre-configured Grafana dashboards showing:

Message Volume: Real-time message throughput

Processing Latency: End-to-end processing delays

Error Rates: Failed message processing

Resource Utilization: CPU, memory, and disk usage

Kafka Metrics: Topic lag, partition distribution

Storage Metrics: MinIO bucket usage and performance

Key Metrics
kafka_server_brokertopicmetrics_messagesinpersec: Messages per second

kafka_server_brokertopicmetrics_bytesinpersec: Bytes per second

kafka_consumer_lag_sum: Consumer lag

python_consumer_batch_processing_time: Batch processing time (if exposed by consumer)

minio_bucket_usage_total_bytes: Storage usage

🧪 Testing
Unit Tests
# Run all unit tests
make test

# Run specific test files
make test-producer
make test-consumer # Assuming tests for the Python consumer
make test-validator

Integration Tests
# Run integration tests
make test-integration

# Test S3 connectivity
make test-s3

Load Testing
# Generate high-volume test data
make load-test

# Monitor performance during load test
make monitor-load

🏭 Production Deployment
Infrastructure Provisioning
# Plan infrastructure changes
make terraform-plan

# Apply infrastructure
make terraform-apply

# Destroy infrastructure (careful!)
make terraform-destroy

CI/CD Pipeline
The project includes GitHub Actions workflows:

Feature Branch CI: Linting, formatting, security scans

Main Branch CI: Full testing, integration tests, Docker builds

CD Pipeline: Automated Terraform deployment

Security Considerations
All secrets managed via environment variables

Network security groups restrict access

Data encryption in transit and at rest

Regular security scanning with Bandit and Safety

📁 Project Structure
.
├── .github/workflows/          # GitHub Actions CI/CD
├── config/                     # Configuration files
│   ├── grafana/               # Grafana dashboards and provisioning
│   ├── jmx-exporter.yml       # JMX metrics configuration
│   └── prometheus.yml         # Prometheus configuration
├── infra/terraform/           # Infrastructure as Code
├── src/                       # Source code
│   ├── producer/              # Kafka producer
│   ├── kafka_consumer_etl/    # Python Kafka Consumer ETL
│   ├── data_validator.py      # Data validation utilities
│   └── historical_data_generator.py
├── tests/                     # Test suite
├── docker-compose.yml         # Local development environment
├── Makefile                   # Automation scripts
└── README.md                  # This file

🔍 Troubleshooting
Common Issues
Kafka Connection Issues

# Check Kafka status
make kafka-status

# View Kafka logs
make kafka-logs

Python Consumer Job Failures

# Check Python consumer logs
make consumer-logs

# Restart Python consumer job
make consumer-restart

MinIO Access Issues

# Test S3 connectivity
make test-s3

# Check MinIO logs
make minio-logs

Memory Issues

# Monitor resource usage
make monitor-resources

# Adjust memory settings in docker-compose.yml

Performance Tuning
Kafka: Adjust num.partitions and replication.factor

Python Consumer: Tune AGGREGATOR_BATCH_SIZE and AGGREGATOR_FLUSH_INTERVAL

MinIO: Configure appropriate storage classes and lifecycle policies

📚 Additional Resources
Apache Kafka Documentation

MinIO Documentation

Grafana Dashboard Creation



