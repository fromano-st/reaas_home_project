"""
Unit tests for IoT event producer.
"""
import pytest
import os
from unittest.mock import Mock, patch
from datetime import datetime, timezone
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.producer.producer import IoTDataGenerator, IoTEventProducer
from src.producer.schema import IoTEvent, DeviceType


class TestIoTDataGenerator:
    """Test cases for IoT data generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = IoTDataGenerator()

    def test_initialization(self):
        """Test generator initialization."""
        assert self.generator is not None
        assert len(self.generator.devices) > 0
        assert len(self.generator.locations) > 0
        assert len(self.generator.device_configs) == len(DeviceType)

    def test_device_generation(self):
        """Test device instance generation."""
        devices = self.generator._generate_devices()

        assert len(devices) > 0

        # Check device structure
        device = devices[0]
        assert "device_id" in device
        assert "device_type" in device
        assert "location" in device
        assert "config" in device

        # Check device types are valid
        for device in devices:
            assert device["device_type"] in DeviceType
            assert device["location"] in self.generator.locations

    def test_metadata_generation(self):
        """Test metadata generation for different device types."""
        for device_type in DeviceType:
            metadata = self.generator._generate_metadata(device_type)

            assert isinstance(metadata, dict)
            assert len(metadata) > 0

            # Check metadata keys match expected keys
            expected_keys = self.generator.device_configs[device_type]["metadata_keys"]
            assert set(metadata.keys()) == set(expected_keys)

    def test_event_generation(self):
        """Test IoT event generation."""
        event = self.generator.generate_event()

        # Check event type
        assert isinstance(event, IoTEvent)

        # Check required fields
        assert event.event_duration > 0
        assert event.device_type in [dt.value for dt in DeviceType]
        assert event.device_id is not None
        assert event.timestamp is not None
        assert event.location in self.generator.locations
        assert event.value is not None
        assert event.unit is not None
        assert 0 <= event.battery_level <= 100
        assert -120 <= event.signal_strength <= -30

    def test_event_value_ranges(self):
        """Test that generated values are within expected ranges."""
        # Generate multiple events to test ranges
        events = [self.generator.generate_event() for _ in range(100)]

        # Group by device type
        events_by_type = {}
        for event in events:
            if event.device_type not in events_by_type:
                events_by_type[event.device_type] = []
            events_by_type[event.device_type].append(event)

        # Check value ranges for each device type
        for device_type, type_events in events_by_type.items():
            config = self.generator.device_configs[device_type]
            min_val, max_val = config["value_range"]

            for event in type_events:
                assert (
                    min_val <= event.value <= max_val
                ), f"Value {event.value} out of range for {device_type}"

    def test_boolean_sensors(self):
        """Test boolean sensor value generation."""
        boolean_types = [
            DeviceType.MOTION_DETECTOR,
            DeviceType.DOOR_SENSOR,
            DeviceType.WINDOW_SENSOR,
        ]

        for _ in range(50):
            event = self.generator.generate_event()
            if event.device_type in boolean_types:
                assert event.value in [
                    0.0,
                    1.0,
                ], f"Boolean sensor should only have 0.0 or 1.0, got {event.value}"


class TestIoTEventProducer:
    """Test cases for IoT event producer."""

    def setup_method(self):
        """Set up test fixtures."""
        self.bootstrap_servers = "localhost:9092"
        self.topic = "test-topic"

    @patch("src.producer.producer.KafkaProducer")
    def test_producer_initialization(self, mock_kafka_producer):
        """Test producer initialization."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = IoTEventProducer(self.bootstrap_servers, self.topic)

        assert producer.topic == self.topic
        assert producer.data_generator is not None
        mock_kafka_producer.assert_called_once()

    @patch("src.producer.producer.KafkaProducer")
    def test_send_event(self, mock_kafka_producer):
        """Test event sending."""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance

        producer = IoTEventProducer(self.bootstrap_servers, self.topic)

        # Create test event
        test_event = IoTEvent(
            event_duration=1.5,
            device_type=DeviceType.TEMPERATURE_SENSOR,
            device_id="test_device_001",
            timestamp=datetime.now(timezone.utc),
            location="Test Room",
            value=25.5,
            unit="°C",
            battery_level=85,
            signal_strength=-65,
            metadata={"test": "data"},
        )

        producer.send_event(test_event)

        # Verify send was called
        mock_producer_instance.send.assert_called_once()
        call_args = mock_producer_instance.send.call_args

        assert call_args[1]["topic"] == self.topic
        assert call_args[1]["key"] == test_event.device_id

        # Verify callbacks were added
        mock_future.add_callback.assert_called_once()
        mock_future.add_errback.assert_called_once()

    @patch("src.producer.producer.KafkaProducer")
    def test_producer_close(self, mock_kafka_producer):
        """Test producer close."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = IoTEventProducer(self.bootstrap_servers, self.topic)
        producer.close()

        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.close.assert_called_once()


class TestEnvironmentVariables:
    """Test environment variable handling."""

    def test_missing_kafka_bootstrap_servers(self):
        """Test error when KAFKA_BOOTSTRAP_SERVERS is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError,
                match="KAFKA_BOOTSTRAP_SERVERS environment variable is required",
            ):
                from src.producer.producer import main

                main()

    def test_missing_kafka_topic(self):
        """Test error when KAFKA_TOPIC is missing."""
        with patch.dict(
            os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}, clear=True
        ):
            with pytest.raises(
                ValueError, match="KAFKA_TOPIC environment variable is required"
            ):
                from src.producer.producer import main

                main()

    def test_missing_producer_interval(self):
        """Test error when PRODUCER_INTERVAL is missing."""
        with patch.dict(
            os.environ,
            {"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", "KAFKA_TOPIC": "test-topic"},
            clear=True,
        ):
            with pytest.raises(
                ValueError, match="PRODUCER_INTERVAL environment variable is required"
            ):
                from src.producer.producer import main

                main()

    def test_invalid_producer_interval(self):
        """Test error when PRODUCER_INTERVAL is invalid."""
        with patch.dict(
            os.environ,
            {
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "KAFKA_TOPIC": "test-topic",
                "PRODUCER_INTERVAL": "invalid",
            },
            clear=True,
        ):
            with pytest.raises(
                ValueError, match="PRODUCER_INTERVAL must be a valid float"
            ):
                from src.producer.producer import main

                main()

    def test_invalid_max_events(self):
        """Test error when MAX_EVENTS is invalid."""
        with patch.dict(
            os.environ,
            {
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "KAFKA_TOPIC": "test-topic",
                "PRODUCER_INTERVAL": "1.0",
                "MAX_EVENTS": "invalid",
            },
            clear=True,
        ):
            with pytest.raises(ValueError, match="MAX_EVENTS must be a valid integer"):
                from src.producer.producer import main

                main()


if __name__ == "__main__":
    pytest.main([__file__])
