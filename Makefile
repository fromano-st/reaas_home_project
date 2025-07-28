TERRAFORM_DIR = infra/terraform

# Load environment variables
include .env
export

# Infrastructure commands
init-terraform:
	@echo "Initializing Terraform..."
	terraform -chdir=$(TERRAFORM_DIR) init

plan-infrastructure:
	@echo "Creating Terraform execution plan..."
	terraform -chdir=$(TERRAFORM_DIR) plan \
	  -var="use_minio=true" \
	  -var="minio_endpoint=$(MINIO_ENDPOINT)" \
	  -var="minio_access_key=$(MINIO_ACCESS_KEY)" \
	  -var="minio_secret_key=$(MINIO_SECRET_KEY)"

apply-infrastructure:
	@echo "Applying Terraform configuration..."
	terraform -chdir=$(TERRAFORM_DIR) apply -auto-approve \
	  -var="use_minio=true" \
	  -var="minio_endpoint=$(MINIO_ENDPOINT)" \
	  -var="minio_access_key=$(MINIO_ACCESS_KEY)" \
	  -var="minio_secret_key=$(MINIO_SECRET_KEY)"

destroy-infrastructure:
	@echo "Destroying Terraform infrastructure..."
	terraform -chdir=$(TERRAFORM_DIR) destroy -auto-approve \
	  -var="use_minio=true" \
	  -var="minio_endpoint=$(MINIO_ENDPOINT)" \
	  -var="minio_access_key=$(MINIO_ACCESS_KEY)" \
	  -var="minio_secret_key=$(MINIO_SECRET_KEY)"

# Docker services
start-infrastructure:
	@echo "Starting infrastructure services..."
	docker-compose up -d zookeeper kafka minio prometheus grafana kafka-ui jmx-exporter
	@echo "Waiting for services to initialize..."
	sleep 30

start-producer:
	@echo "Starting IoT producer..."
	docker-compose up -d iot-producer

start-streaming:
	@echo "Starting Spark streaming job..."
	docker-compose up -d spark-streaming

start-all:
	@echo "Starting all services..."
	docker-compose up -d

stop-all:
	@echo "Stopping all services..."
	docker-compose down

# Setup commands
setup-infrastructure: start-infrastructure init-terraform apply-infrastructure

setup-complete: setup-infrastructure start-producer start-streaming
	@echo "Complete streaming ETL pipeline is now running!"
	@echo "Services available at:"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Grafana: http://localhost:3000 (admin/$(GRAFANA_ADMIN_PASSWORD))"
	@echo "  - Prometheus: http://localhost:9090"

# Testing commands (improved with script integration)
test-s3:
	@echo "Testing S3/MinIO connectivity..."
	./scripts/setup-integration-tests.sh s3-test

test-producer:
	@echo "Testing Kafka producer..."
	docker-compose build iot-producer
	docker-compose run --rm -e MAX_EVENTS=10 iot-producer

test-kafka-consumer:
	@echo "Testing Kafka consumer..."
	./scripts/setup-integration-tests.sh kafka-test

# Integration testing commands (NEW)
setup-integration-tests:
	@echo "Setting up integration test environment..."
	./scripts/setup-integration-tests.sh setup

run-integration-tests:
	@echo "Running integration tests..."
	./scripts/setup-integration-tests.sh integration

full-integration-tests:
	@echo "Running full integration test suite..."
	./scripts/setup-integration-tests.sh full

integration-test-status:
	@echo "Checking integration test service status..."
	./scripts/setup-integration-tests.sh status

integration-test-logs:
	@echo "Showing integration test logs..."
	./scripts/setup-integration-tests.sh logs

cleanup-integration-tests:
	@echo "Cleaning up integration test environment..."
	./scripts/setup-integration-tests.sh cleanup

# Data generation
generate-historical-data:
	@echo "Generating historical data..."
	cd src && python historical_data_generator.py

# Monitoring
logs-producer:
	docker-compose logs -f iot-producer

logs-streaming:
	docker-compose logs -f spark-streaming

logs-all:
	docker-compose logs -f

# Maintenance
clean-data:
	@echo "Cleaning all data volumes..."
	docker-compose down -v

clean-all: clean-data destroy-infrastructure
	@echo "Cleaning Docker images..."
	docker-compose down --rmi all

restart-producer:
	docker-compose restart iot-producer

restart-streaming:
	docker-compose restart spark-streaming

# Development
build-all:
	@echo "Building all Docker images..."
	docker-compose build

# Status
status:
	@echo "Service status:"
	docker-compose ps

# CI/CD Integration (NEW)
ci-integration-tests:
	@echo "Running CI-style integration tests..."
	@echo "This replicates the CI workflow locally"
	./scripts/setup-integration-tests.sh full

# Help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Production Commands:"
	@echo "  setup-complete           - Set up and start the complete pipeline"
	@echo "  setup-infrastructure     - Set up infrastructure only"
	@echo "  start-all               - Start all services"
	@echo "  stop-all                - Stop all services"
	@echo ""
	@echo "Testing Commands:"
	@echo "  test-s3                 - Test S3/MinIO connectivity"
	@echo "  test-producer           - Test Kafka producer"
	@echo "  test-kafka-consumer     - Test Kafka consumer"
	@echo ""
	@echo "Integration Testing Commands:"
	@echo "  setup-integration-tests  - Setup integration test environment"
	@echo "  run-integration-tests    - Run integration tests only"
	@echo "  full-integration-tests   - Run complete integration test suite"
	@echo "  integration-test-status  - Check integration test service status"
	@echo "  integration-test-logs    - Show integration test logs"
	@echo "  cleanup-integration-tests - Clean up integration test environment"
	@echo "  ci-integration-tests     - Run CI-style integration tests locally"
	@echo ""
	@echo "Development Commands:"
	@echo "  generate-historical-data - Generate week of historical data"
	@echo "  logs-producer           - Show producer logs"
	@echo "  logs-streaming          - Show streaming job logs"
	@echo "  logs-all                - Show all service logs"
	@echo "  build-all               - Build all Docker images"
	@echo "  clean-all               - Clean everything"
	@echo "  status                  - Show service status"
	@echo "  help                    - Show this help"

.PHONY: init-terraform plan-infrastructure apply-infrastructure destroy-infrastructure \
        start-infrastructure start-producer start-streaming start-all stop-all \
        setup-infrastructure setup-complete test-s3 test-producer test-kafka-consumer \
        setup-integration-tests run-integration-tests full-integration-tests \
        integration-test-status integration-test-logs cleanup-integration-tests \
        ci-integration-tests generate-historical-data logs-producer logs-streaming logs-all \
        clean-data clean-all restart-producer restart-streaming build-all status help