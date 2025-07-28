# Integration Tests CI/CD Guide

This guide explains how to adapt your CI workflow to handle integration tests similar to your Makefile functionality.

## Overview

The improved CI workflow replicates your local Makefile commands in the CI environment, specifically:

- `make setup-infrastructure` → CI infrastructure setup steps
- `docker-compose up s3-tester` → S3 connectivity tests
- `docker-compose up -d iot-producer` → IoT producer startup
- `docker-compose up kafka-consumer-tester` → Kafka consumer tests

## Key Improvements

### 1. Infrastructure Setup Replication

The CI workflow now properly replicates your `make setup-infrastructure` command:

```yaml
- name: Setup Infrastructure (equivalent to make setup-infrastructure)
  run: |
    echo "🚀 Starting infrastructure services..."
    
    # Start core infrastructure services (equivalent to make start-infrastructure)
    docker-compose up -d zookeeper kafka minio prometheus grafana kafka-ui jmx-exporter
    
    echo "⏳ Waiting for services to initialize..."
    sleep 30
```

### 2. Service Health Checks

Added robust health checks that wait for services to be fully ready:

```yaml
- name: Wait for services to be healthy
  run: |
    # Wait for Kafka to be ready
    timeout 120 bash -c 'until nc -z localhost 9092; do echo "Waiting for Kafka..."; sleep 5; done'
    
    # Wait for MinIO to be ready
    timeout 120 bash -c 'until curl -f http://localhost:9000/minio/health/live; do echo "Waiting for MinIO..."; sleep 5; done'
```

### 3. Test Execution Sequence

The workflow now follows your exact local testing sequence:

1. **S3 Tester**: `docker-compose run --rm s3-tester`
2. **IoT Producer**: `docker-compose up -d iot-producer`
3. **Kafka Consumer Tester**: `docker-compose run --rm kafka-consumer-tester`
4. **Integration Tests**: `python -m pytest test_integration.py`

### 4. Proper Environment Configuration

Creates a comprehensive `.env` file with all necessary configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-events-test

# Producer Configuration
PRODUCER_INTERVAL=1.0
MAX_EVENTS=10

# MinIO/S3 Configuration
MINIO_ENDPOINT=http://localhost:9000
AWS_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
# ... and more
```

## Files Created

### 1. `ci-main-improved.yml`
- Complete CI workflow with proper integration test setup
- Replicates Makefile functionality in CI environment
- Includes proper health checks and service management
- Adds comprehensive cleanup procedures

### 2. `scripts/setup-integration-tests.sh`
- Standalone script for integration test management
- Can be used both locally and in CI
- Provides granular control over test execution
- Includes colored output and error handling

### 3. `Makefile.improved`
- Enhanced Makefile with integration test commands
- Integrates with the new shell script
- Provides CI-compatible local testing
- Maintains backward compatibility

### 4. `INTEGRATION_TESTS_GUIDE.md`
- This documentation file
- Explains all improvements and usage

## Usage

### Local Development

#### Using the Enhanced Makefile:
```bash
# Run complete integration test suite (equivalent to CI)
make ci-integration-tests

# Setup integration test environment only
make setup-integration-tests

# Run individual test components
make test-s3
make test-kafka-consumer
make run-integration-tests

# Check status and logs
make integration-test-status
make integration-test-logs

# Cleanup
make cleanup-integration-tests
```

#### Using the Script Directly:
```bash
# Run complete integration test suite
./scripts/setup-integration-tests.sh full

# Run individual components
./scripts/setup-integration-tests.sh setup
./scripts/setup-integration-tests.sh s3-test
./scripts/setup-integration-tests.sh producer
./scripts/setup-integration-tests.sh kafka-test
./scripts/setup-integration-tests.sh integration

# Utilities
./scripts/setup-integration-tests.sh status
./scripts/setup-integration-tests.sh logs
./scripts/setup-integration-tests.sh cleanup
```

### CI Environment

The improved CI workflow (`ci-main-improved.yml`) automatically:

1. **Sets up infrastructure** - Starts all required services
2. **Waits for readiness** - Ensures services are healthy before testing
3. **Runs S3 tests** - Verifies MinIO connectivity
4. **Starts producer** - Launches IoT data producer
5. **Tests Kafka consumer** - Verifies message consumption
6. **Runs integration tests** - Executes full test suite
7. **Collects artifacts** - Saves test results and logs
8. **Cleans up** - Removes all test infrastructure

## Key Differences from Original CI

### Before (Original CI Issues):
- Used GitHub Actions services instead of docker-compose
- Incomplete service dependency management
- Missing health checks for service readiness
- Didn't replicate Makefile functionality
- Limited integration test coverage

### After (Improved CI):
- ✅ Uses docker-compose for consistent environment
- ✅ Proper service startup sequence with health checks
- ✅ Replicates exact Makefile functionality
- ✅ Comprehensive integration test execution
- ✅ Proper cleanup and resource management
- ✅ Detailed logging and status reporting

## Environment Variables

The integration tests use these key environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-events-test

# Producer
PRODUCER_INTERVAL=1.0
MAX_EVENTS=10

# MinIO/S3
MINIO_ENDPOINT=http://localhost:9000
AWS_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# S3 Buckets
AWS_S3_BUCKET_LANDING=test-bucket-landing
AWS_S3_BUCKET_DATA=test-bucket-data
```

## Troubleshooting

### Common Issues:

1. **Services not ready**: The workflow includes health checks, but if services fail to start:
   - Check Docker resource limits
   - Verify port availability
   - Review service logs

2. **Test timeouts**: If tests timeout:
   - Increase wait times in health checks
   - Check service dependencies
   - Verify network connectivity

3. **Permission issues**: If file permission errors occur:
   - Ensure proper directory permissions (777 for spark-checkpoints)
   - Check Docker volume mounts

### Debugging Commands:

```bash
# Check service status
docker-compose ps

# View service logs
docker-compose logs [service-name]

# Test connectivity manually
nc -z localhost 9092  # Kafka
curl -f http://localhost:9000/minio/health/live  # MinIO
```

## Migration Steps

To migrate from your current CI to the improved version:

1. **Replace CI workflow**:
   ```bash
   cp ci-main-improved.yml .github/workflows/ci-main.yml
   ```

2. **Update Makefile** (optional):
   ```bash
   cp Makefile.improved Makefile
   ```

3. **Add integration test script**:
   ```bash
   mkdir -p scripts
   cp scripts/setup-integration-tests.sh scripts/
   chmod +x scripts/setup-integration-tests.sh
   ```

4. **Test locally**:
   ```bash
   make ci-integration-tests
   ```

5. **Commit and push** to trigger CI

## Benefits

1. **Consistency**: Local and CI environments now match exactly
2. **Reliability**: Proper health checks prevent race conditions
3. **Maintainability**: Centralized integration test logic
4. **Debugging**: Better logging and status reporting
5. **Flexibility**: Granular control over test execution
6. **Efficiency**: Proper cleanup prevents resource leaks

The improved workflow ensures your integration tests run reliably in CI while maintaining the same functionality as your local Makefile commands.