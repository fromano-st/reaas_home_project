# Real-Time Streaming ETL Pipeline

This repository contains the solution for the Real-Time Streaming ETL take-home assignment, focusing on designing a production-grade streaming data pipeline using open-source technologies, integrating cloud services, and implementing live observability.
the original assignment is saved under take_home_assignment.md

## Implemented Components and Deviations

The following sections outline the status of each task and any specific choices or deviations made during the implementation.

### 1. Ingestion
**Status**: Completed

The streaming ingestion system has been successfully deployed, including a custom JSON producer that periodically emits records.

### 2. Processing Engine
**Status**: Completed

The streaming framework to consume from the message broker and perform ETL (parsing, filtering, windowed transformations, and aggregation/deduplication) has been implemented.

**Note on Implementation**: Due to issues encountered with Spark configuration during development, the processing engine was implemented in Python instead of Spark Structured Streaming.

### 3. Sink to Spark-based Storage
**Status**: Completed

Processed data is successfully written to a storage system in Parquet format.

**Note on Implementation**: To avoid costs associated with AWS S3 for this Proof-of-Concept (POC) and due to its strong similarities with S3, MinIO was chosen as the storage solution instead of AWS S3.

### 4. Observability and Live Monitoring
**Status**: Partially Completed

A monitoring setup, including Grafana and Prometheus, has been deployed to provide real-time dashboards with metrics on ingestion, processing, and data throughput.

**Note on Implementation**: While the services appear to be running correctly, some configuration issues are preventing some data from being displayed on the Grafana dashboard. Further investigation is needed to resolve this.

### 5. Repository and Automation
**Status**: Completed

All code is version-controlled in this GitHub repository.

**Automation**: GitHub Actions have been utilized for Continuous Integration (CI), including unit and integration tests. Shell scripts are provided for one-command setup.

### 6. Spark Query
**Status**:  Partially Completed

A Spark query was developed to calculate the 95th percentile of event duration. This calculation is performed per device type, per day, with outliers excluded. The query also filters for device types that have a sufficient number of distinct events.

There were some errors during the execution of the Spark job.