"""
Kafka consumer test for IoT event data consumption.
"""
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaTimeoutError
from pydantic import BaseModel, Field, ValidationError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IoTEventValidator(BaseModel):
    """
    Validator for IoT event data to ensure consumed messages match expected schema.
    """

    event_duration: float = Field(
        ..., description="Duration of the event in seconds", ge=0.1, le=3600.0
    )
    device_type: str = Field(..., description="Type of the device")
    device_id: str = Field(..., description="Unique device identifier")
    timestamp: str = Field(..., description="Event timestamp")
    location: str = Field(..., description="Device location")
    value: float = Field(..., description="Sensor measurement value")
    unit: str = Field(..., description="Unit of measurement")
    battery_level: Optional[int] = Field(
        None, description="Battery level percentage", ge=0, le=100
    )
    signal_strength: Optional[int] = Field(
        None, description="Signal strength in dBm", ge=-120, le=-30
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Additional device metadata"
    )


class KafkaConsumerTester:
    """Test Kafka connectivity and message consumption from iot-producer."""

    def __init__(self):
        """Initialize Kafka consumer."""
        # Configuration from environment - no defaults, raise errors if missing
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        self.topic = os.getenv("KAFKA_TOPIC")
        if not self.topic:
            raise ValueError("KAFKA_TOPIC environment variable is required")

        # Test configuration
        self.max_messages = int(os.getenv("KAFKA_TEST_MAX_MESSAGES", "10"))
        self.timeout_seconds = int(os.getenv("KAFKA_TEST_TIMEOUT", "30"))

        logger.info(
            f"Initializing Kafka consumer with bootstrap servers: {self.bootstrap_servers}"
        )
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Max messages to consume: {self.max_messages}")
        logger.info(f"Timeout: {self.timeout_seconds} seconds")

        # -tester_1   | 2025-07-28 14:36:06,190 - __main__ - ERROR - Connection failed: 'KafkaConsumer' object has no attribute 'list_consumer_groups'

        # Consumer configuration
        self.consumer = None
        self.messages_consumed = 0
        self.valid_messages = 0
        self.invalid_messages = 0
        self.device_types_seen = set()
        self.locations_seen = set()
        self.running = True

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def test_connection(self) -> bool:
        """Test basic Kafka connection."""
        try:
            logger.info("Testing Kafka connection...")

            # Create a temporary consumer to test connection
            test_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                group_id="kafka-test-connection",
            )

            # Get cluster metadata
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

            consumer_groups = admin_client.list_consumer_groups()
            logger.info(
                f"Connection successful - able to retrieve cluster metadata {consumer_groups}"
            )

            # Check if our topic exists
            topics = test_consumer.topics()
            if self.topic in topics:
                logger.info(f"Topic '{self.topic}' found in cluster")
            else:
                logger.warning(
                    f"Topic '{self.topic}' not found in cluster. Available topics: {list(topics)}"
                )

            test_consumer.close()
            return True

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def create_consumer(self) -> bool:
        """Create and configure the Kafka consumer."""
        try:
            logger.info("Creating Kafka consumer...")

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="latest",  # Start from latest messages
                enable_auto_commit=True,
                group_id="kafka-consumer-tester",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                consumer_timeout_ms=self.timeout_seconds * 1000,
                max_poll_records=1,  # Process one message at a time for testing
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

            logger.info("Kafka consumer created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            return False

    def validate_message(self, message_value: Dict[str, Any]) -> bool:
        """Validate a consumed message against the expected schema."""
        try:
            # Validate using Pydantic model
            validated_event = IoTEventValidator(**message_value)

            # Track statistics
            self.device_types_seen.add(validated_event.device_type)
            self.locations_seen.add(validated_event.location)

            logger.info(
                f"✓ Valid message from device {validated_event.device_id} "
                f"({validated_event.device_type}) at {validated_event.location}"
            )
            logger.info(
                f"  Value: {validated_event.value} {validated_event.unit}, "
                f"Duration: {validated_event.event_duration}s"
            )

            return True

        except ValidationError as e:
            logger.error(f"✗ Invalid message schema: {e}")
            logger.error(f"  Message content: {json.dumps(message_value, indent=2)}")
            return False
        except Exception as e:
            logger.error(f"✗ Error validating message: {e}")
            return False

    def consume_messages(self) -> bool:
        """Consume and validate messages from the Kafka topic."""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return False

        try:
            logger.info(f"Starting to consume messages from topic '{self.topic}'...")
            logger.info(
                f"Will consume up to {self.max_messages} messages or timeout after {self.timeout_seconds}s"
            )

            start_time = datetime.now(timezone.utc)

            for message in self.consumer:
                if not self.running:
                    logger.info("Shutdown signal received, stopping consumption")
                    break

                self.messages_consumed += 1

                logger.info(f"\n--- Message {self.messages_consumed} ---")
                logger.info(f"Partition: {message.partition}, Offset: {message.offset}")
                logger.info(f"Key: {message.key}")
                logger.info(
                    f"Timestamp: {datetime.fromtimestamp(message.timestamp/1000, tz=timezone.utc).isoformat()}"
                )

                # Validate message content
                if self.validate_message(message.value):
                    self.valid_messages += 1
                else:
                    self.invalid_messages += 1

                # Check if we've consumed enough messages
                if self.messages_consumed >= self.max_messages:
                    logger.info(f"Reached maximum message count: {self.max_messages}")
                    break

                # Check timeout
                elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                if elapsed_time >= self.timeout_seconds:
                    logger.info(f"Reached timeout: {self.timeout_seconds} seconds")
                    break

            return True

        except KafkaTimeoutError:
            logger.warning("Consumer timed out waiting for messages")
            return self.messages_consumed > 0  # Success if we got at least one message
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return False

    def print_statistics(self):
        """Print consumption statistics."""
        logger.info("\n" + "=" * 50)
        logger.info("KAFKA CONSUMER TEST STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Total messages consumed: {self.messages_consumed}")
        logger.info(f"Valid messages: {self.valid_messages}")
        logger.info(f"Invalid messages: {self.invalid_messages}")

        if self.messages_consumed > 0:
            success_rate = (self.valid_messages / self.messages_consumed) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")

        if self.device_types_seen:
            logger.info(f"Device types seen: {sorted(list(self.device_types_seen))}")

        if self.locations_seen:
            logger.info(f"Locations seen: {sorted(list(self.locations_seen))}")

        logger.info("=" * 50)

    def run_comprehensive_test(self) -> bool:
        """Run comprehensive Kafka consumer tests."""
        logger.info("Starting comprehensive Kafka consumer tests...")

        success = True

        # Test 1: Connection
        if not self.test_connection():
            success = False
            return success

        # Test 2: Create consumer
        if not self.create_consumer():
            success = False
            return success

        # Test 3: Consume messages
        if not self.consume_messages():
            success = False

        # Print statistics
        self.print_statistics()

        # Evaluate results
        if self.messages_consumed == 0:
            logger.error("No messages were consumed - this could indicate:")
            logger.error("  1. No messages are being produced to the topic")
            logger.error("  2. Topic doesn't exist")
            logger.error("  3. Consumer configuration issues")
            success = False
        elif self.valid_messages == 0:
            logger.error(
                "No valid messages were found - schema validation failed for all messages"
            )
            success = False
        elif self.invalid_messages > 0:
            logger.warning(f"Some messages ({self.invalid_messages}) failed validation")
            # Don't mark as failure if we got some valid messages

        if success:
            logger.info("✓ Kafka consumer tests passed successfully!")
        else:
            logger.error("✗ Some Kafka consumer tests failed!")

        return success

    def close(self):
        """Close the consumer and cleanup resources."""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("Consumer closed")


def main():
    """Main function to run Kafka consumer tests."""
    logger.info("Starting Kafka consumer connectivity tests...")

    try:
        tester = KafkaConsumerTester()
        success = tester.run_comprehensive_test()

        if success:
            logger.info("Kafka consumer testing completed successfully")
            sys.exit(0)
        else:
            logger.error("Kafka consumer testing failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        try:
            if "tester" in locals():
                tester.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
