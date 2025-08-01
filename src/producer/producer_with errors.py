"""
Kafka producer for IoT event data simulation.
"""
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

from kafka import KafkaProducer

from src.producer.schema import IoTEvent, DeviceType, SCHEMA_DOCUMENTATION


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints."""

    def __init__(self, producer_instance, *args, **kwargs):
        self.producer_instance = producer_instance
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests for health checks."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            health_status = {
                "status": "healthy"
                if self.producer_instance.is_healthy()
                else "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": "iot-producer",
                "version": "1.0.0",
            }

            self.wfile.write(json.dumps(health_status).encode())
        elif self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

            metrics = self.producer_instance.get_metrics()
            self.wfile.write(metrics.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress default HTTP server logging."""
        pass


class HealthCheckServer:
    """Simple HTTP server for health checks."""

    def __init__(self, producer_instance, port=8080):
        self.producer_instance = producer_instance
        self.port = port
        self.server = None
        self.thread = None

    def start(self):
        """Start the health check server."""
        handler = lambda *args, **kwargs: HealthCheckHandler(  # noqa:E731
            self.producer_instance, *args, **kwargs
        )
        self.server = HTTPServer(("0.0.0.0", self.port), handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Health check server started on port {self.port}")

    def stop(self):
        """Stop the health check server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.thread:
            self.thread.join()
        logger.info("Health check server stopped")


class IoTDataGenerator:
    """Generates realistic IoT event data."""

    def __init__(self):
        """Initialize the data generator with device configurations."""
        self.locations = [
            "Living Room",
            "Kitchen",
            "Bedroom",
            "Bathroom",
            "Office",
            "Garage",
            "Basement",
            "Attic",
            "Garden",
            "Patio",
        ]

        self.device_configs = {
            DeviceType.TEMPERATURE_SENSOR: {
                "value_range": (15.0, 35.0),
                "unit": "°C",
                "duration_range": (1.0, 5.0),
                "metadata_keys": ["firmware_version", "calibration_date"],
            },
            DeviceType.HUMIDITY_SENSOR: {
                "value_range": (30.0, 90.0),
                "unit": "%",
                "duration_range": (1.5, 4.0),
                "metadata_keys": ["sensor_model", "last_maintenance"],
            },
            DeviceType.PRESSURE_SENSOR: {
                "value_range": (980.0, 1050.0),
                "unit": "hPa",
                "duration_range": (2.0, 6.0),
                "metadata_keys": ["altitude_compensation", "accuracy_class"],
            },
            DeviceType.MOTION_DETECTOR: {
                "value_range": (0.0, 1.0),
                "unit": "boolean",
                "duration_range": (0.5, 10.0),
                "metadata_keys": ["detection_range", "sensitivity"],
            },
            DeviceType.LIGHT_SENSOR: {
                "value_range": (0.0, 100000.0),
                "unit": "lux",
                "duration_range": (1.0, 3.0),
                "metadata_keys": ["spectral_range", "response_time"],
            },
            DeviceType.SMART_THERMOSTAT: {
                "value_range": (16.0, 28.0),
                "unit": "°C",
                "duration_range": (5.0, 30.0),
                "metadata_keys": ["schedule_active", "learning_mode"],
            },
            DeviceType.DOOR_SENSOR: {
                "value_range": (0.0, 1.0),
                "unit": "boolean",
                "duration_range": (0.1, 2.0),
                "metadata_keys": ["magnetic_strength", "tamper_detection"],
            },
            DeviceType.WINDOW_SENSOR: {
                "value_range": (0.0, 1.0),
                "unit": "boolean",
                "duration_range": (0.1, 3.0),
                "metadata_keys": ["contact_type", "weather_sealed"],
            },
        }

        # Generate device instances
        self.devices = self._generate_devices()

    def _generate_devices(self) -> List[Dict[str, Any]]:
        """Generate a list of device instances."""
        devices = []
        device_count_per_type = 3  # 3 devices per type

        for device_type in DeviceType:
            for i in range(device_count_per_type):
                device_id = f"{device_type.value}_{i+1:03d}_{random.choice(self.locations).lower().replace(' ', '_')}"
                devices.append(
                    {
                        "device_id": device_id,
                        "device_type": device_type,
                        "location": random.choice(self.locations),
                        "config": self.device_configs[device_type],
                    }
                )

        return devices

    def _generate_metadata(self, device_type: DeviceType) -> Dict[str, Any]:
        """Generate realistic metadata for a device type."""
        config = self.device_configs[device_type]
        metadata = {}

        for key in config["metadata_keys"]:
            if key == "firmware_version":
                metadata[
                    key
                ] = f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            elif key == "calibration_date":
                date = datetime.now() - timedelta(days=random.randint(1, 365))
                metadata[key] = date.strftime("%Y-%m-%d")
            elif key == "sensor_model":
                metadata[key] = f"SEN-{random.randint(1000, 9999)}"
            elif key == "last_maintenance":
                date = datetime.now() - timedelta(days=random.randint(1, 90))
                metadata[key] = date.strftime("%Y-%m-%d")
            elif key == "altitude_compensation":
                metadata[key] = random.choice([True, False])
            elif key == "accuracy_class":
                metadata[key] = random.choice(["A", "B", "C"])
            elif key == "detection_range":
                metadata[key] = f"{random.randint(5, 15)}m"
            elif key == "sensitivity":
                metadata[key] = random.choice(["low", "medium", "high"])
            elif key == "spectral_range":
                metadata[key] = "380-780nm"
            elif key == "response_time":
                metadata[key] = f"{random.randint(10, 100)}ms"
            elif key == "schedule_active":
                metadata[key] = random.choice([True, False])
            elif key == "learning_mode":
                metadata[key] = random.choice([True, False])
            elif key == "magnetic_strength":
                metadata[key] = f"{random.randint(50, 200)}G"
            elif key == "tamper_detection":
                metadata[key] = random.choice([True, False])
            elif key == "contact_type":
                metadata[key] = random.choice(["magnetic", "mechanical"])
            elif key == "weather_sealed":
                metadata[key] = random.choice([True, False])

        return metadata

    def generate_event(self) -> IoTEvent:
        """Generate a single IoT event."""
        device = random.choice(self.devices)
        config = device["config"]

        # Generate realistic values based on device type
        if device["device_type"] in [
            DeviceType.MOTION_DETECTOR,
            DeviceType.DOOR_SENSOR,
            DeviceType.WINDOW_SENSOR,
        ]:
            # Boolean sensors
            value = float(random.choice([0, 1]))
        else:
            # Continuous sensors
            min_val, max_val = config["value_range"]
            value = round(random.uniform(min_val, max_val), 2)

        # Generate event duration
        min_duration, max_duration = config["duration_range"]
        event_duration = round(random.uniform(min_duration, max_duration), 2)

        # Generate battery and signal strength
        battery_level = random.randint(20, 100)  # Realistic battery levels
        signal_strength = random.randint(-90, -40)  # Realistic signal strength

        return IoTEvent(
            event_duration=event_duration,
            device_type=device["device_type"],
            device_id=device["device_id"],
            timestamp=datetime.now(timezone.utc),
            location=device["location"],
            value=value,
            unit=config["unit"],
            battery_level=battery_level,
            signal_strength=signal_strength,
            metadata=self._generate_metadata(device["device_type"]),
        )


class IoTEventProducer:
    """Kafka producer for IoT events."""

    def __init__(self, bootstrap_servers: str, topic: str):
        """Initialize the Kafka producer."""
        self.topic = topic
        self.data_generator = IoTDataGenerator()

        # Configure Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=1000,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
            enable_idempotence=True,  # Exactly-once semantics
            compression_type="gzip",
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
        )

        # Health check tracking
        self.last_successful_send = datetime.now(timezone.utc)
        self.total_events_sent = 0
        self.total_errors = 0
        self.is_running = False
        self.message_counter = 0  # Add counter for message tracking

        logger.info("Modified producer: Will generate errors every 50 messages")

        logger.info(f"Initialized Kafka producer for topic: {topic}")
        logger.info(f"Bootstrap servers: {bootstrap_servers}")

    def send_event(self, event: IoTEvent) -> None:
        """Send a single event to Kafka."""
        self.message_counter += 1
        try:
            if self.message_counter % 50 == 0:
                raise RuntimeError("INTENTIONAL PRODUCER ERROR (Every 50th message)")
            # Convert event to dictionary
            event_dict = event.model_dump()

            # Use device_id as the key for partitioning
            key = event.device_id

            # Send to Kafka
            future = self.producer.send(topic=self.topic, key=key, value=event_dict)

            # Add callback for success/error handling
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

            logger.debug(
                f"Sent event from device {event.device_id} ({event.device_type})"
            )

        except Exception as e:
            # Handle both intentional and real errors
            self.total_errors += 1
            logger.error(f"Error sending event: {str(e)}")

            # Special handling for intentional errors
            if "INTENTIONAL" in str(e):
                logger.warning("Simulated error injected. This is expected behavior")

    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        self.last_successful_send = datetime.now(timezone.utc)
        self.total_events_sent += 1
        logger.debug(
            f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
        )

    def _on_send_error(self, exception):
        """Callback for message delivery errors."""
        self.total_errors += 1
        logger.error(f"Message delivery failed: {exception}")

    def is_healthy(self) -> bool:
        """Check if the producer is healthy."""
        if not self.is_running:
            return False

        # Check if we've had a successful send in the last 5 minutes
        time_since_last_send = datetime.now(timezone.utc) - self.last_successful_send
        if time_since_last_send.total_seconds() > 300:  # 5 minutes
            return False

        # Check error rate (should be less than 10%)
        if self.total_events_sent > 0:
            error_rate = self.total_errors / (
                self.total_events_sent + self.total_errors
            )
            if error_rate > 0.1:
                return False

        return True

    def get_metrics(self) -> str:
        """Get producer metrics in Prometheus format."""
        metrics = []
        metrics.append(
            "# HELP iot_producer_events_sent_total Total number of events sent"
        )
        metrics.append("# TYPE iot_producer_events_sent_total counter")
        metrics.append(f"iot_producer_events_sent_total {self.total_events_sent}")

        metrics.append("# HELP iot_producer_errors_total Total number of errors")
        metrics.append("# TYPE iot_producer_errors_total counter")
        metrics.append(f"iot_producer_errors_total {self.total_errors}")

        metrics.append("# HELP iot_producer_is_running Producer running status")
        metrics.append("# TYPE iot_producer_is_running gauge")
        metrics.append(f"iot_producer_is_running {1 if self.is_running else 0}")

        time_since_last_send = datetime.now(timezone.utc) - self.last_successful_send
        metrics.append(
            "# HELP iot_producer_seconds_since_last_send Seconds since last successful send"
        )
        metrics.append("# TYPE iot_producer_seconds_since_last_send gauge")
        metrics.append(
            f"iot_producer_seconds_since_last_send {time_since_last_send.total_seconds()}"
        )

        return "\n".join(metrics)

    def start_producing(
        self, interval_seconds: float = 1.0, max_events: int = None
    ) -> None:
        """Start producing events at regular intervals."""
        logger.info(f"Starting event production with {interval_seconds}s interval")

        if max_events:
            logger.info(f"Will produce maximum {max_events} events")

        event_count = 0
        self.is_running = True

        try:
            while True:
                # Generate and send event
                event = self.data_generator.generate_event()
                self.send_event(event)

                event_count += 1

                if event_count % 100 == 0:
                    logger.info(f"Produced {event_count} events")

                # Check if we've reached the maximum
                if max_events and event_count >= max_events:
                    logger.info(f"Reached maximum event count: {max_events}")
                    break

                # Wait before next event
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping producer...")
        except Exception as e:
            logger.error(f"Error in event production: {e}")
        finally:
            self.close()

    def close(self):
        """Close the producer and flush remaining messages."""
        logger.info("Closing producer...")
        self.is_running = False
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


def main():
    """Main function to run the producer."""
    # Configuration from environment variables - no defaults, raise errors if missing
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

    topic = os.getenv("KAFKA_TOPIC")
    if not topic:
        raise ValueError("KAFKA_TOPIC environment variable is required")

    interval_str = os.getenv("PRODUCER_INTERVAL")
    if not interval_str:
        raise ValueError("PRODUCER_INTERVAL environment variable is required")

    try:
        interval = float(interval_str)
    except ValueError:
        raise ValueError("PRODUCER_INTERVAL must be a valid float")

    max_events = os.getenv("MAX_EVENTS")
    if max_events:
        try:
            max_events = int(max_events)
        except ValueError:
            raise ValueError("MAX_EVENTS must be a valid integer")

    logger.info("Starting IoT Event Producer")
    logger.info("Configuration:")
    logger.info(f"  Bootstrap servers: {bootstrap_servers}")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Interval: {interval}s")
    logger.info(f"  Max events: {max_events or 'unlimited'}")

    # Print schema documentation
    logger.info("Event Schema:")
    logger.info(json.dumps(SCHEMA_DOCUMENTATION, indent=2))

    # Create and start producer
    producer = IoTEventProducer(bootstrap_servers, topic)
    producer.start_producing(interval, max_events)


if __name__ == "__main__":
    main()
