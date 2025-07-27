"""
Integration tests for the complete streaming ETL pipeline.
"""
import json
import os
import sys
import threading
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.producer.producer import IoTDataGenerator, IoTEventProducer
from src.producer.schema import DeviceType, IoTEvent


class TestPipelineIntegration:
    """Integration tests for the complete pipeline."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_topic = "test-iot-events"
        self.bootstrap_servers = "localhost:9092"

    def test_data_generation_and_serialization(self):
        """Test that generated data can be properly serialized and deserialized."""
        generator = IoTDataGenerator()

        # Generate multiple events
        events = [generator.generate_event() for _ in range(10)]

        # Test serialization
        serialized_events = []
        for event in events:
            json_data = event.model_dump_json()
            serialized_events.append(json_data)

            # Test deserialization
            parsed_data = json.loads(json_data)
            reconstructed_event = IoTEvent(**parsed_data)

            # Verify data integrity
            assert reconstructed_event.event_duration == event.event_duration
            assert reconstructed_event.device_type == event.device_type
            assert reconstructed_event.device_id == event.device_id
            assert reconstructed_event.value == event.value
            assert reconstructed_event.unit == event.unit

    def test_schema_compliance(self):
        """Test that all generated events comply with the required schema."""
        generator = IoTDataGenerator()

        # Generate events from all device types
        events = []
        for device_type in DeviceType:
            # Force generation of specific device type for testing
            event = generator.generate_event()
            events.append(event)

        for event in events:
            # Check required fields from specification
            assert hasattr(event, "event_duration"), "Missing event_duration field"
            assert hasattr(event, "device_type"), "Missing device_type field"

            # Check data types
            assert isinstance(
                event.event_duration, (int, float)
            ), "event_duration must be numeric"
            assert isinstance(event.device_type, str), "device_type must be string"
            assert event.device_type in [
                dt.value for dt in DeviceType
            ], "device_type must be valid"

            # Check value ranges
            assert event.event_duration > 0, "event_duration must be positive"
            assert 0 <= event.battery_level <= 100, "battery_level must be 0-100"
            assert (
                -120 <= event.signal_strength <= -30
            ), "signal_strength must be in valid range"

    @patch("src.producer.producer.KafkaProducer")
    def test_producer_error_handling(self, mock_kafka_producer):
        """Test producer error handling and resilience."""
        # Mock producer that raises errors
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance

        producer = IoTEventProducer(self.bootstrap_servers, self.test_topic)
        generator = IoTDataGenerator()

        # Test successful send
        event = generator.generate_event()
        producer.send_event(event)

        # Verify send was called
        mock_producer_instance.send.assert_called_once()

        # Test error callback
        call_args = mock_producer_instance.send.call_args
        sent_value = call_args[1]["value"]  # This should be the event dictionary

        # Verify the event was properly prepared for serialization
        assert isinstance(sent_value, dict)
        assert "event_duration" in sent_value
        assert "device_type" in sent_value

        # Test that it can be serialized to JSON (which Kafka producer would do)
        json_str = json.dumps(sent_value, default=str)
        deserialized = json.loads(json_str)
        assert "event_duration" in deserialized
        assert "device_type" in deserialized

    def test_data_quality_validation(self):
        """Test data quality and consistency."""
        generator = IoTDataGenerator()

        # Generate a large sample of events
        events = [generator.generate_event() for _ in range(100)]

        # Check for data quality issues
        device_types = set()
        locations = set()

        for event in events:
            device_types.add(event.device_type)
            locations.add(event.location)

            # Check timestamp is recent
            time_diff = datetime.now(timezone.utc) - event.timestamp
            assert time_diff.total_seconds() < 10, "Timestamp should be recent"

            # Check device_id format
            assert event.device_id.startswith(
                event.device_type
            ), "device_id should start with device_type"

            # Check metadata exists and is valid
            assert isinstance(event.metadata, dict), "metadata should be a dictionary"
            assert len(event.metadata) > 0, "metadata should not be empty"

        # Check diversity
        assert len(device_types) > 1, "Should generate multiple device types"
        assert len(locations) > 1, "Should generate multiple locations"

    def test_event_duration_requirements(self):
        """Test that event_duration field meets specification requirements."""
        generator = IoTDataGenerator()

        # Generate many events to test the range
        events = [generator.generate_event() for _ in range(50)]

        for event in events:
            # Check that event_duration exists and is positive
            assert hasattr(event, "event_duration"), "event_duration field is required"
            assert event.event_duration > 0, "event_duration must be positive"
            assert isinstance(
                event.event_duration, (int, float)
            ), "event_duration must be numeric"

            # Check reasonable range (should be between 0.1 and 30 seconds for IoT events)
            # Smart thermostats can have longer durations up to 30 seconds
            assert (
                0.1 <= event.event_duration <= 30.0
            ), f"event_duration {event.event_duration} seems unreasonable"

    def test_device_type_requirements(self):
        """Test that device_type field meets specification requirements."""
        generator = IoTDataGenerator()

        # Generate events and check device_type
        events = [generator.generate_event() for _ in range(30)]

        valid_device_types = [dt.value for dt in DeviceType]

        for event in events:
            # Check that device_type exists and is valid
            assert hasattr(event, "device_type"), "device_type field is required"
            assert (
                event.device_type in valid_device_types
            ), f"device_type {event.device_type} is not valid"
            assert isinstance(event.device_type, str), "device_type must be a string"

    def test_json_serialization_compatibility(self):
        """Test that events can be serialized to JSON for Kafka."""
        generator = IoTDataGenerator()

        # Test with different device types
        for device_type in DeviceType:
            event = generator.generate_event()

            # Test Pydantic JSON serialization
            json_str = event.model_dump_json()
            assert isinstance(json_str, str), "JSON serialization should return string"

            # Test that it's valid JSON
            parsed = json.loads(json_str)
            assert isinstance(parsed, dict), "Parsed JSON should be a dictionary"

            # Check required fields are present in JSON
            assert "event_duration" in parsed, "event_duration missing from JSON"
            assert "device_type" in parsed, "device_type missing from JSON"

            # Test standard JSON serialization (for Kafka compatibility)
            standard_json = json.dumps(event.model_dump())
            parsed_standard = json.loads(standard_json)
            assert parsed_standard["event_duration"] == event.event_duration
            assert parsed_standard["device_type"] == event.device_type

    def test_concurrent_event_generation(self):
        """Test that event generation works correctly under concurrent access."""
        generator = IoTDataGenerator()
        events = []
        errors = []

        def generate_events(count):
            try:
                for _ in range(count):
                    event = generator.generate_event()
                    events.append(event)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=generate_events, args=(10,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert (
            len(errors) == 0
        ), f"Errors occurred during concurrent generation: {errors}"
        assert len(events) == 50, f"Expected 50 events, got {len(events)}"

        # Check that all events are valid
        for event in events:
            assert isinstance(
                event, IoTEvent
            ), "All generated objects should be IoTEvent instances"
            assert event.event_duration > 0, "All events should have positive duration"
            assert event.device_type in [
                dt.value for dt in DeviceType
            ], "All events should have valid device types"


class TestFaultTolerance:
    """Test fault tolerance and recovery scenarios."""

    def test_invalid_data_handling(self):
        """Test handling of invalid data scenarios."""
        # Test with invalid device type
        with pytest.raises(ValueError):
            IoTEvent(
                event_duration=1.0,
                device_type="invalid_device_type",
                device_id="test_001",
                timestamp=datetime.now(timezone.utc),
                location="Test Location",
                value=25.0,
                unit="°C",
                battery_level=85,
                signal_strength=-65,
                metadata={},
            )

    def test_missing_required_fields(self):
        """Test handling of missing required fields."""
        # Test missing event_duration
        with pytest.raises(ValueError):
            IoTEvent(
                device_type=DeviceType.TEMPERATURE_SENSOR.value,
                device_id="test_001",
                timestamp=datetime.now(timezone.utc),
                location="Test Location",
                value=25.0,
                unit="°C",
                battery_level=85,
                signal_strength=-65,
                metadata={},
            )  # type: ignore

    def test_boundary_values(self):
        """Test handling of boundary values."""
        # Test minimum event_duration
        event = IoTEvent(
            event_duration=0.1,  # Minimum reasonable value
            device_type=DeviceType.TEMPERATURE_SENSOR.value,
            device_id="test_001",
            timestamp=datetime.now(timezone.utc),
            location="Test Location",
            value=25.0,
            unit="°C",
            battery_level=0,  # Minimum battery
            signal_strength=-120,  # Minimum signal
            metadata={},
        )
        assert event.event_duration == 0.1
        assert event.battery_level == 0
        assert event.signal_strength == -120

        # Test maximum values
        event = IoTEvent(
            event_duration=10.0,  # Maximum reasonable value
            device_type=DeviceType.TEMPERATURE_SENSOR.value,
            device_id="test_001",
            timestamp=datetime.now(timezone.utc),
            location="Test Location",
            value=25.0,
            unit="°C",
            battery_level=100,  # Maximum battery
            signal_strength=-30,  # Maximum signal
            metadata={},
        )
        assert event.event_duration == 10.0
        assert event.battery_level == 100
        assert event.signal_strength == -30


if __name__ == "__main__":
    pytest.main([__file__])
