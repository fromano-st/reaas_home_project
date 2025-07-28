#!/usr/bin/env python3
"""
Metrics exporter for Kafka streaming application
Exposes custom metrics for processing failures, retries, and success rates
"""

import time
import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricsExporter:
    """Custom metrics exporter for streaming application"""

    def __init__(self, port=8080):
        self.port = port
        self.lock = Lock()

        # Define metrics
        self.processing_success_total = Counter(
            "processing_success_total",
            "Total number of successfully processed messages",
            ["topic", "partition"],
        )

        self.processing_failures_total = Counter(
            "processing_failures_total",
            "Total number of processing failures",
            ["topic", "partition", "error_type"],
        )

        self.processing_retries_total = Counter(
            "processing_retries_total",
            "Total number of processing retries",
            ["topic", "partition"],
        )

        self.processing_duration = Histogram(
            "processing_duration_seconds",
            "Time spent processing messages",
            ["topic", "partition"],
        )

        self.current_failure_rate = Gauge(
            "current_failure_rate",
            "Current failure rate (failures / total processed)",
            ["topic"],
        )

        self.consumer_lag = Gauge(
            "consumer_lag_messages",
            "Consumer lag in number of messages",
            ["topic", "partition", "consumer_group"],
        )

        # Internal counters for rate calculations
        self._success_count = {}
        self._failure_count = {}

    def start_server(self):
        """Start the metrics HTTP server"""
        try:
            start_http_server(self.port)
            logger.info(f"Metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            raise

    def record_success(self, topic, partition="0"):
        """Record a successful message processing"""
        with self.lock:
            self.processing_success_total.labels(topic=topic, partition=partition).inc()

            # Update internal counter for rate calculation
            key = f"{topic}:{partition}"
            self._success_count[key] = self._success_count.get(key, 0) + 1
            self._update_failure_rate(topic)

    def record_failure(self, topic, partition="0", error_type="unknown"):
        """Record a processing failure"""
        with self.lock:
            self.processing_failures_total.labels(
                topic=topic, partition=partition, error_type=error_type
            ).inc()

            # Update internal counter for rate calculation
            key = f"{topic}:{partition}"
            self._failure_count[key] = self._failure_count.get(key, 0) + 1
            self._update_failure_rate(topic)

    def record_retry(self, topic, partition="0"):
        """Record a processing retry"""
        with self.lock:
            self.processing_retries_total.labels(topic=topic, partition=partition).inc()

    def record_processing_time(self, topic, partition="0", duration_seconds=0):
        """Record processing duration"""
        with self.lock:
            self.processing_duration.labels(topic=topic, partition=partition).observe(
                duration_seconds
            )

    def update_consumer_lag(self, topic, partition, consumer_group, lag_messages):
        """Update consumer lag metric"""
        with self.lock:
            self.consumer_lag.labels(
                topic=topic, partition=partition, consumer_group=consumer_group
            ).set(lag_messages)

    def _update_failure_rate(self, topic):
        """Update the current failure rate for a topic"""
        # Calculate failure rate across all partitions for the topic
        total_success = sum(
            count
            for key, count in self._success_count.items()
            if key.startswith(f"{topic}:")
        )
        total_failures = sum(
            count
            for key, count in self._failure_count.items()
            if key.startswith(f"{topic}:")
        )

        total_processed = total_success + total_failures
        if total_processed > 0:
            failure_rate = total_failures / total_processed
            self.current_failure_rate.labels(topic=topic).set(failure_rate)
        else:
            self.current_failure_rate.labels(topic=topic).set(0)


# Global metrics instance
metrics_exporter = None


def get_metrics_exporter(port=8080):
    """Get or create the global metrics exporter instance"""
    global metrics_exporter
    if metrics_exporter is None:
        metrics_exporter = MetricsExporter(port)
    return metrics_exporter


def start_metrics_server(port=8080):
    """Start the metrics server"""
    exporter = get_metrics_exporter(port)
    exporter.start_server()
    return exporter


if __name__ == "__main__":
    # Example usage
    exporter = start_metrics_server(8080)

    # Simulate some metrics
    import random

    logger.info("Starting metrics simulation...")

    try:
        while True:
            # Simulate processing events
            topic = random.choice(["user-events", "system-logs", "transactions"])
            partition = str(random.randint(0, 2))

            # Simulate success/failure
            if random.random() < 0.9:  # 90% success rate
                start_time = time.time()
                time.sleep(random.uniform(0.01, 0.1))  # Simulate processing time
                processing_time = time.time() - start_time

                exporter.record_success(topic, partition)
                exporter.record_processing_time(topic, partition, processing_time)
            else:
                error_type = random.choice(
                    ["timeout", "parse_error", "validation_error"]
                )
                exporter.record_failure(topic, partition, error_type)

                # Sometimes retry
                if random.random() < 0.5:
                    exporter.record_retry(topic, partition)

            # Simulate consumer lag
            lag = random.randint(0, 1000)
            exporter.update_consumer_lag(topic, partition, "streaming-consumer", lag)

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Metrics simulation stopped")
