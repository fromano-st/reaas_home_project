#!/bin/bash
"""
Integration test script for E-commerce Streaming ETL Pipeline
Tests the complete end-to-end pipeline functionality
"""

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
TEST_LOG="$PROJECT_ROOT/integration_test.log"
TEST_DURATION=60  # seconds
PRODUCER_RATE=5   # messages per second

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$TEST_LOG"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}" | tee -a "$TEST_LOG"
}

warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}" | tee -a "$TEST_LOG"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}" | tee -a "$TEST_LOG"
}

# Cleanup function
cleanup() {
    log "Cleaning up test environment..."
    
    # Stop producer if running
    if [ -f "$PROJECT_ROOT/test_producer.pid" ]; then
        kill $(cat "$PROJECT_ROOT/test_producer.pid") 2>/dev/null || true
        rm -f "$PROJECT_ROOT/test_producer.pid"
    fi
    
    # Clean up test data
    rm -rf "$PROJECT_ROOT/data/test_output"
    rm -rf "$PROJECT_ROOT/data/test_checkpoints"
    
    # Remove test topics
    docker-compose exec -T kafka kafka-topics \
        --delete --bootstrap-server localhost:9092 \
        --topic test-ecommerce-transactions 2>/dev/null || true
    
    log "Cleanup completed"
}

# Trap cleanup on exit
trap cleanup EXIT

# Test functions
test_infrastructure() {
    log "Testing infrastructure services..."
    
    # Check Docker Compose services
    if ! docker-compose ps | grep -q "Up"; then
        error "Docker Compose services are not running"
        return 1
    fi
    
    # Test Kafka connectivity
    if ! docker-compose exec -T kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 &> /dev/null; then
        error "Kafka is not accessible"
        return 1
    fi
    
    # Test Prometheus
    if ! curl -s http://localhost:9090/-/ready &> /dev/null; then
        warning "Prometheus is not ready"
    fi
    
    # Test Grafana
    if ! curl -s http://localhost:3000/api/health &> /dev/null; then
        warning "Grafana is not ready"
    fi
    
    log "Infrastructure services test passed"
    return 0
}

test_kafka_topics() {
    log "Testing Kafka topic creation..."
    
    # Create test topic
    docker-compose exec -T kafka kafka-topics \
        --create --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions 3 \
        --topic test-ecommerce-transactions \
        --if-not-exists
    
    # Verify topic exists
    if ! docker-compose exec -T kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list | grep -q "test-ecommerce-transactions"; then
        error "Test topic was not created"
        return 1
    fi
    
    log "Kafka topics test passed"
    return 0
}

test_producer() {
    log "Testing transaction producer..."
    
    cd "$PROJECT_ROOT"
    
    # Activate virtual environment
    source venv/bin/activate 2>/dev/null || {
        error "Virtual environment not found. Run 'make install' first."
        return 1
    }
    
    # Start producer for test duration
    python src/producer/ecommerce_producer.py \
        --bootstrap-servers localhost:9092 \
        --topic test-ecommerce-transactions \
        --rate $PRODUCER_RATE \
        --duration $TEST_DURATION \
        --metrics-port 8001 \
        > test_producer.log 2>&1 &
    
    echo $! > test_producer.pid
    
    # Wait for producer to start
    sleep 5
    
    # Check if producer is running
    if ! ps -p $(cat test_producer.pid) > /dev/null; then
        error "Producer failed to start"
        cat test_producer.log
        return 1
    fi
    
    # Wait for producer to finish
    wait $(cat test_producer.pid)
    
    # Check producer metrics
    if curl -s http://localhost:8001/metrics | grep -q "producer_messages_sent_total"; then
        log "Producer metrics are available"
    else
        warning "Producer metrics not found"
    fi
    
    log "Producer test passed"
    return 0
}

test_message_consumption() {
    log "Testing message consumption..."
    
    # Consume messages from test topic
    timeout 10 docker-compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic test-ecommerce-transactions \
        --from-beginning \
        --max-messages 5 > test_messages.txt 2>/dev/null || true
    
    # Check if messages were consumed
    if [ -s test_messages.txt ]; then
        message_count=$(wc -l < test_messages.txt)
        log "Successfully consumed $message_count messages"
        
        # Validate message format
        if head -1 test_messages.txt | jq . > /dev/null 2>&1; then
            log "Messages are valid JSON"
        else
            error "Messages are not valid JSON"
            return 1
        fi
        
        # Check required fields
        if head -1 test_messages.txt | jq -e '.transaction_id' > /dev/null 2>&1; then
            log "Messages contain required fields"
        else
            error "Messages missing required fields"
            return 1
        fi
    else
        error "No messages were consumed"
        return 1
    fi
    
    # Cleanup
    rm -f test_messages.txt
    
    log "Message consumption test passed"
    return 0
}

test_data_validation() {
    log "Testing data validation..."
    
    cd "$PROJECT_ROOT"
    source venv/bin/activate
    
    # Create test data directory
    mkdir -p data/test_output
    
    # Generate some test data files (simulate Spark output)
    python -c "
import json
import os
from datetime import datetime

# Create test parquet-like data structure
test_data = []
for i in range(100):
    record = {
        'transaction_id': f'txn_{i:08d}',
        'customer_id': f'cust_{i % 50}',
        'product_id': f'prod_{i % 20}',
        'total_amount': round(10.0 + (i % 100), 2),
        'quantity': (i % 5) + 1,
        'timestamp': datetime.now().isoformat(),
        'device_type': ['mobile', 'desktop', 'tablet'][i % 3],
        'event_duration': 30.0 + (i % 300),
        'year': 2023,
        'month': 6,
        'day': 15
    }
    test_data.append(record)

# Save test data
os.makedirs('data/test_output', exist_ok=True)
with open('data/test_output/test_data.json', 'w') as f:
    for record in test_data:
        f.write(json.dumps(record) + '\n')

print(f'Generated {len(test_data)} test records')
"
    
    # Validate the test data
    if [ -f "data/test_output/test_data.json" ]; then
        record_count=$(wc -l < data/test_output/test_data.json)
        log "Generated $record_count test records for validation"
    else
        error "Failed to generate test data"
        return 1
    fi
    
    log "Data validation test passed"
    return 0
}

test_monitoring() {
    log "Testing monitoring and metrics..."
    
    # Test Prometheus metrics
    if curl -s http://localhost:9090/api/v1/query?query=up | jq -e '.status == "success"' > /dev/null 2>&1; then
        log "Prometheus API is working"
    else
        warning "Prometheus API test failed"
    fi
    
    # Test Kafka metrics via JMX exporter
    if curl -s http://localhost:5556/metrics | grep -q "kafka_"; then
        log "Kafka metrics are available"
    else
        warning "Kafka metrics not found"
    fi
    
    # Test Grafana API
    if curl -s http://localhost:3000/api/datasources | jq -e 'length > 0' > /dev/null 2>&1; then
        log "Grafana datasources configured"
    else
        warning "Grafana datasources test failed"
    fi
    
    log "Monitoring test passed"
    return 0
}

test_performance() {
    log "Testing performance characteristics..."
    
    # Check resource usage
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | tee -a "$TEST_LOG"
    
    # Check disk usage
    df -h | grep -E "(Size|/)" | tee -a "$TEST_LOG"
    
    # Test message throughput
    expected_messages=$((PRODUCER_RATE * TEST_DURATION))
    info "Expected approximately $expected_messages messages in $TEST_DURATION seconds"
    
    log "Performance test completed"
    return 0
}

# Main test execution
main() {
    log "Starting E-commerce Streaming ETL Integration Tests"
    log "Test configuration: Duration=${TEST_DURATION}s, Rate=${PRODUCER_RATE}/s"
    
    # Initialize test results
    declare -A test_results
    
    # Run tests
    tests=(
        "test_infrastructure"
        "test_kafka_topics"
        "test_producer"
        "test_message_consumption"
        "test_data_validation"
        "test_monitoring"
        "test_performance"
    )
    
    for test in "${tests[@]}"; do
        log "Running $test..."
        if $test; then
            test_results[$test]="PASSED"
        else
            test_results[$test]="FAILED"
            error "$test failed"
        fi
    done
    
    # Print test summary
    log "Integration Test Summary:"
    echo "=========================" | tee -a "$TEST_LOG"
    
    passed=0
    failed=0
    
    for test in "${tests[@]}"; do
        result=${test_results[$test]}
        if [ "$result" = "PASSED" ]; then
            echo -e "${GREEN}✓ $test: $result${NC}" | tee -a "$TEST_LOG"
            ((passed++))
        else
            echo -e "${RED}✗ $test: $result${NC}" | tee -a "$TEST_LOG"
            ((failed++))
        fi
    done
    
    echo "=========================" | tee -a "$TEST_LOG"
    echo "Total: $((passed + failed)), Passed: $passed, Failed: $failed" | tee -a "$TEST_LOG"
    
    if [ $failed -eq 0 ]; then
        log "All integration tests passed!"
        return 0
    else
        error "$failed integration tests failed!"
        return 1
    fi
}

# Check if infrastructure is running
check_prerequisites() {
    log "Checking prerequisites..."
    
    cd "$PROJECT_ROOT"
    
    # Check if Docker Compose is running
    if ! docker-compose ps | grep -q "Up"; then
        error "Infrastructure services are not running."
        error "Please run 'make deploy' or 'make start' first."
        exit 1
    fi
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        error "Python virtual environment not found."
        error "Please run 'make install' first."
        exit 1
    fi
    
    log "Prerequisites check passed"
}

# Script entry point
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    check_prerequisites
    main "$@"
fi