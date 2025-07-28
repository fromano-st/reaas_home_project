#!/bin/bash

# Integration Test Setup Script
# This script replicates the Makefile functionality for integration tests
# Can be used both locally and in CI environments

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for service to be ready
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-30}
    local attempt=1

    print_status "Waiting for $service_name to be ready at $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            print_success "$service_name is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to start within $((max_attempts * 2)) seconds"
    return 1
}

# Function to wait for HTTP service
wait_for_http_service() {
    local service_name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1

    print_status "Waiting for $service_name to be ready at $url..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s $url > /dev/null 2>&1; then
            print_success "$service_name is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to start within $((max_attempts * 2)) seconds"
    return 1
}

# Function to create required directories
create_directories() {
    print_status "Creating required directories..."
    mkdir -p tmp/spark-checkpoints
    mkdir -p config
    chmod 777 tmp/spark-checkpoints
    print_success "Directories created"
}

# Function to create minimal config files
create_config_files() {
    print_status "Creating configuration files..."
    
    # Create minimal prometheus config
    cat > config/prometheus.yml << EOF
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
EOF

    # Create minimal JMX exporter config
    cat > config/jmx-exporter.yml << EOF
rules:
  - pattern: ".*"
EOF

    print_success "Configuration files created"
}

# Function to create test environment file
create_test_env() {
    print_status "Creating test environment file..."
    
    cat > .env << EOF
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-events-test

# Producer Configuration
PRODUCER_INTERVAL=1.0
MAX_EVENTS=10

# Spark Configuration
CHECKPOINT_LOCATION=/tmp/spark-checkpoints-test
OUTPUT_PATH=s3a://test-bucket-data/processed/

# MinIO/S3 Configuration
MINIO_ENDPOINT=http://localhost:9000
AWS_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# S3 Buckets
AWS_S3_BUCKET_LANDING=test-bucket-landing
AWS_S3_BUCKET_DATA=test-bucket-data

# Historical Data Configuration
HISTORICAL_DAYS=1

# Grafana Configuration
GRAFANA_ADMIN_PASSWORD=admin123
EOF

    print_success "Test environment file created"
}

# Function to setup infrastructure (equivalent to make setup-infrastructure)
setup_infrastructure() {
    print_status "Setting up infrastructure services..."
    
    # Start core infrastructure services
    docker-compose up -d zookeeper kafka minio prometheus grafana kafka-ui jmx-exporter
    
    print_status "Waiting for services to initialize..."
    sleep 30
    
    # Wait for critical services
    wait_for_service "Kafka" "localhost" "9092"
    wait_for_http_service "MinIO" "http://localhost:9000/minio/health/live"
    
    print_success "Infrastructure setup completed"
}

# Function to run S3 tests
run_s3_tests() {
    print_status "Running S3 connectivity tests..."
    docker-compose build s3-tester
    docker-compose run --rm s3-tester
    print_success "S3 tests completed"
}

# Function to start IoT producer
start_iot_producer() {
    print_status "Starting IoT Producer..."
    docker-compose up -d iot-producer
    
    print_status "Allowing producer to generate some data..."
    sleep 30
    print_success "IoT Producer started"
}

# Function to run Kafka consumer tests
run_kafka_consumer_tests() {
    print_status "Running Kafka Consumer tests..."
    docker-compose build kafka-consumer-tester
    docker-compose run --rm kafka-consumer-tester
    print_success "Kafka Consumer tests completed"
}

# Function to run integration tests
run_integration_tests() {
    print_status "Running integration tests..."
    cd tests
    python -m pytest test_integration.py -v --tb=short
    cd ..
    print_success "Integration tests completed"
}

# Function to show service status
show_service_status() {
    print_status "Current service status:"
    docker-compose ps
}

# Function to show logs
show_logs() {
    local service=${1:-""}
    if [ -n "$service" ]; then
        print_status "Showing logs for $service:"
        docker-compose logs $service
    else
        print_status "Showing logs for all services:"
        docker-compose logs
    fi
}

# Function to cleanup
cleanup() {
    print_status "Cleaning up infrastructure..."
    docker-compose down -v
    docker system prune -f
    print_success "Cleanup completed"
}

# Function to run full integration test suite
run_full_integration_tests() {
    print_status "Starting full integration test suite..."
    
    # Setup
    create_directories
    create_config_files
    create_test_env
    
    # Infrastructure
    setup_infrastructure
    
    # Tests
    run_s3_tests
    start_iot_producer
    run_kafka_consumer_tests
    run_integration_tests
    
    # Status
    show_service_status
    show_logs "iot-producer"
    
    print_success "Full integration test suite completed!"
}

# Main script logic
case "${1:-full}" in
    "setup")
        create_directories
        create_config_files
        create_test_env
        setup_infrastructure
        ;;
    "s3-test")
        run_s3_tests
        ;;
    "producer")
        start_iot_producer
        ;;
    "kafka-test")
        run_kafka_consumer_tests
        ;;
    "integration")
        run_integration_tests
        ;;
    "status")
        show_service_status
        ;;
    "logs")
        show_logs "${2:-}"
        ;;
    "cleanup")
        cleanup
        ;;
    "full")
        run_full_integration_tests
        ;;
    "help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  setup       - Setup infrastructure only"
        echo "  s3-test     - Run S3 connectivity tests"
        echo "  producer    - Start IoT producer"
        echo "  kafka-test  - Run Kafka consumer tests"
        echo "  integration - Run integration tests"
        echo "  status      - Show service status"
        echo "  logs [svc]  - Show logs (optionally for specific service)"
        echo "  cleanup     - Clean up all services"
        echo "  full        - Run complete integration test suite (default)"
        echo "  help        - Show this help"
        ;;
    *)
        print_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac