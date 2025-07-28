# Streaming ETL Pipeline Implementation Todo

## 1. Environment Setup and Configuration
- [x] Create .env file for sensitive configuration
- [x] Update docker-compose.yml to use environment variables from .env
- [x] Create proper directory structure for the project
- [x] Update Makefile to use proper terraform directory structure
- [x] Remove default values from environment variables and add proper error handling

## 2. Kafka Producer Implementation
- [x] Create producer source code with JSON data generation
- [x] Implement IoT/event data simulation with event_duration and device_type fields
- [x] Add schema description documentation
- [x] Configure producer for periodic data emission
- [x] Add error handling and logging to producer

## 3. Spark Structured Streaming Implementation
- [x] Create Spark streaming job for Kafka consumption
- [x] Implement ETL transformations (parsing, filtering, windowed operations)
- [x] Add aggregation logic (average value per device per minute)
- [x] Implement fault-tolerance and error handling
- [x] Configure checkpointing for stream processing

## 4. Storage Integration
- [x] Update terraform configuration for MinIO bucket setup
- [x] Create Spark job to write processed data to MinIO in Parquet format
- [x] Implement data validation and testing scripts
- [x] Create historical data population script (1 week of data)

## 5. CI/CD and Repository Structure
- [x] Create GitHub Actions CD workflow with terraform apply
- [x] Add linting and formatting for Python and Terraform
- [x] Implement unit testing with pytest
- [x] Add comprehensive README and runbook

## 6. CI/CD Workflow Updates
- [x] Remove Docker login step from all CI/CD workflows
- [x] Remove Docker push commands from all CI/CD workflows
- [x] Update ci-main.yml to remove docker-build-and-push job
- [x] Update ci-feature.yml to keep only docker build (no push)
- [x] Verify workflows still function correctly after changes

## 7. Monitoring and Observability
- [x] Update Grafana dashboard to include message volume over time metrics
- [x] Add lag/latency metrics by partition/topic to Grafana dashboard
- [x] Add processing failures/retries metrics to Grafana dashboard
- [x] Verify Prometheus configuration for metrics collection
- [x] Test JMX metrics for Kafka monitoring
- [x] Add health check endpoints for services

## 8. Testing and Validation
- [ ] Verify unit tests for all components work correctly
- [ ] Add integration tests for the complete pipeline
- [ ] Validate data quality and schema compliance
- [ ] Test fault tolerance and recovery scenarios

## 9. Documentation
- [ ] Check and Update README with setup instructions
- [ ] Document schema and data flow
- [ ] Add troubleshooting guide
- [ ] Check and update runbook for operations