# Operations Runbook

This runbook provides step-by-step procedures for operating and maintaining the Real-Time Streaming ETL Pipeline.

## 🚨 Emergency Procedures

### System Down - Complete Outage

1. **Check Infrastructure Status**
   ```bash
   make status-all
   docker-compose ps
   ```

2. **Restart All Services**
   ```bash
   make down
   make up
   ```

3. **Verify Service Health**
   ```bash
   make health-check
   ```

4. **Check Monitoring Dashboards**
   - Grafana: http://localhost:3000
   - Verify all metrics are flowing

### Kafka Cluster Issues

1. **Check Kafka Status**
   ```bash
   make kafka-status
   docker-compose logs kafka
   ```

2. **Common Kafka Issues:**
   - **Broker Down**: Restart with `make kafka-restart`
   - **Topic Issues**: Recreate topics with `make kafka-topics-create`
   - **Consumer Lag**: Check consumer group status

3. **Kafka Recovery**
   ```bash
   # Stop Kafka
   docker-compose stop kafka zookeeper
   
   # Clean data (if necessary)
   docker-compose down -v
   
   # Restart
   make kafka-up
   ```

### Spark Streaming Job Failures

1. **Check Streaming Job Status**
   ```bash
   make streaming-status
   docker-compose logs spark-streaming
   ```

2. **Common Issues:**
   - **Checkpoint Corruption**: Clear checkpoints and restart
   - **Memory Issues**: Increase Spark memory allocation
   - **Schema Evolution**: Update schema handling

3. **Restart Streaming Job**
   ```bash
   make streaming-stop
   make streaming-start
   ```

### Storage (MinIO) Issues

1. **Check MinIO Status**
   ```bash
   make minio-status
   docker-compose logs minio
   ```

2. **Test S3 Connectivity**
   ```bash
   make test-s3
   ```

3. **MinIO Recovery**
   ```bash
   # Restart MinIO
   make minio-restart
   
   # Verify buckets exist
   make minio-buckets-create
   ```

## 📊 Monitoring and Alerting

### Key Metrics to Monitor

1. **Message Throughput**
   - Metric: `kafka_server_brokertopicmetrics_messagesinpersec`
   - Alert: < 10 messages/sec for > 5 minutes

2. **Consumer Lag**
   - Metric: `kafka_consumer_lag_sum`
   - Alert: > 1000 messages lag

3. **Processing Latency**
   - Metric: `spark_streaming_batch_processing_time`
   - Alert: > 30 seconds processing time

4. **Error Rate**
   - Metric: `kafka_server_brokertopicmetrics_failedproducerequestspersec`
   - Alert: > 1% error rate

5. **Storage Usage**
   - Metric: `minio_bucket_usage_total_bytes`
   - Alert: > 80% disk usage

### Grafana Dashboard Checks

Daily monitoring checklist:

- [ ] Message volume trends
- [ ] Processing latency within SLA
- [ ] No error spikes
- [ ] Storage growth rate normal
- [ ] All services healthy

## 🔧 Maintenance Procedures

### Daily Maintenance

1. **Health Check**
   ```bash
   make health-check-all
   ```

2. **Log Review**
   ```bash
   make logs-summary
   ```

3. **Disk Space Check**
   ```bash
   df -h
   docker system df
   ```

### Weekly Maintenance

1. **Performance Review**
   - Review Grafana dashboards
   - Check for performance degradation
   - Analyze consumer lag trends

2. **Log Rotation**
   ```bash
   make logs-rotate
   ```

3. **Backup Verification**
   ```bash
   make backup-verify
   ```

### Monthly Maintenance

1. **Capacity Planning**
   - Review storage growth
   - Analyze throughput trends
   - Plan for scaling needs

2. **Security Updates**
   ```bash
   make security-update
   ```

3. **Performance Tuning**
   - Review and adjust Kafka partitions
   - Optimize Spark configurations
   - Update resource allocations

## 🔄 Operational Procedures

### Scaling Operations

#### Scale Kafka Partitions
```bash
# Increase partitions for topic
kafka-topics.sh --bootstrap-server kafka:29092 \
  --alter --topic iot-events --partitions 6
```

#### Scale Spark Resources
```bash
# Update docker-compose.yml
services:
  spark-streaming:
    environment:
      - SPARK_DRIVER_MEMORY=2g
      - SPARK_EXECUTOR_MEMORY=4g
      - SPARK_EXECUTOR_CORES=2
```

#### Scale Storage
```bash
# Add new MinIO volumes in docker-compose.yml
volumes:
  - minio_data1:/data1
  - minio_data2:/data2
```

### Data Management

#### Historical Data Cleanup
```bash
# Clean old data (older than 30 days)
make data-cleanup --days=30
```

#### Data Backup
```bash
# Backup processed data
make backup-data --date=$(date +%Y-%m-%d)
```

#### Data Restore
```bash
# Restore from backup
make restore-data --backup-date=2025-01-15
```

### Configuration Changes

#### Update Producer Configuration
1. Edit `.env` file
2. Restart producer: `make producer-restart`
3. Verify changes: `make producer-status`

#### Update Streaming Job Configuration
1. Edit `src/streaming/streaming_etl.py`
2. Rebuild image: `make streaming-build`
3. Restart job: `make streaming-restart`

#### Update Monitoring Configuration
1. Edit `config/prometheus.yml` or `config/grafana/`
2. Restart monitoring: `make monitoring-restart`
3. Verify dashboards: Check Grafana UI

## 🐛 Troubleshooting Guide

### Performance Issues

#### High Consumer Lag
1. **Symptoms**: Messages backing up in Kafka
2. **Diagnosis**:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
     --describe --group spark-streaming-group
   ```
3. **Solutions**:
   - Increase Spark parallelism
   - Add more Kafka partitions
   - Optimize processing logic

#### Slow Processing
1. **Symptoms**: High batch processing times
2. **Diagnosis**: Check Spark UI and logs
3. **Solutions**:
   - Increase Spark resources
   - Optimize transformations
   - Adjust batch intervals

#### Memory Issues
1. **Symptoms**: OutOfMemory errors
2. **Diagnosis**: Check container memory usage
3. **Solutions**:
   - Increase container memory limits
   - Optimize data structures
   - Implement data sampling

### Data Quality Issues

#### Schema Validation Failures
1. **Symptoms**: Parsing errors in logs
2. **Diagnosis**: Check data validator logs
3. **Solutions**:
   - Update schema definitions
   - Implement schema evolution
   - Add data cleansing logic

#### Missing Data
1. **Symptoms**: Gaps in processed data
2. **Diagnosis**: Check producer and consumer logs
3. **Solutions**:
   - Verify producer connectivity
   - Check Kafka retention settings
   - Implement data recovery procedures

### Network Issues

#### Kafka Connectivity
1. **Symptoms**: Connection timeouts
2. **Diagnosis**: Test network connectivity
3. **Solutions**:
   - Check firewall rules
   - Verify DNS resolution
   - Update connection strings

#### MinIO Access Issues
1. **Symptoms**: S3 operation failures
2. **Diagnosis**: Test S3 connectivity
3. **Solutions**:
   - Verify credentials
   - Check endpoint configuration
   - Test network connectivity

## 📋 Checklists

### Deployment Checklist

Pre-deployment:
- [ ] Code reviewed and approved
- [ ] Tests passing
- [ ] Configuration validated
- [ ] Backup completed

Deployment:
- [ ] Deploy infrastructure changes
- [ ] Update application code
- [ ] Restart services
- [ ] Verify health checks

Post-deployment:
- [ ] Monitor for errors
- [ ] Validate data flow
- [ ] Check performance metrics
- [ ] Update documentation

### Incident Response Checklist

Detection:
- [ ] Alert received
- [ ] Severity assessed
- [ ] Stakeholders notified

Investigation:
- [ ] Logs reviewed
- [ ] Metrics analyzed
- [ ] Root cause identified

Resolution:
- [ ] Fix implemented
- [ ] Services restored
- [ ] Monitoring verified

Post-incident:
- [ ] Incident documented
- [ ] Lessons learned captured
- [ ] Preventive measures implemented

## 📞 Escalation Procedures

### Severity Levels

**P1 - Critical (Complete Outage)**
- Response time: 15 minutes
- Escalation: Immediate to on-call engineer
- Communication: Hourly updates

**P2 - High (Partial Outage)**
- Response time: 1 hour
- Escalation: Within 2 hours if unresolved
- Communication: Every 2 hours

**P3 - Medium (Performance Degradation)**
- Response time: 4 hours
- Escalation: Within 8 hours if unresolved
- Communication: Daily updates

**P4 - Low (Minor Issues)**
- Response time: Next business day
- Escalation: Within 3 days if unresolved
- Communication: Weekly updates

### Contact Information

- **On-call Engineer**: [Contact details]
- **Team Lead**: [Contact details]
- **Infrastructure Team**: [Contact details]
- **Security Team**: [Contact details]

## 📚 Reference Commands

### Quick Reference

```bash
# System Status
make status-all
make health-check

# Service Management
make up / make down
make restart-all

# Monitoring
make logs-tail
make metrics-check

# Data Operations
make data-validate
make data-backup
make data-restore

# Troubleshooting
make debug-kafka
make debug-spark
make debug-minio
```

### Log Locations

- **Kafka**: `docker-compose logs kafka`
- **Spark**: `docker-compose logs spark-streaming`
- **MinIO**: `docker-compose logs minio`
- **Producer**: `docker-compose logs producer`
- **Monitoring**: `docker-compose logs prometheus grafana`

---

**Remember**: Always test procedures in a non-production environment first!