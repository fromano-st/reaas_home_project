"""
Unit and integration tests for the HistoricalDataGenerator class.
"""
import pytest
import json
import os
import sys
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
import warnings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.historical_data_generator import HistoricalDataGenerator
from src.producer.schema import IoTEvent


class TestHistoricalDataGeneratorInit:
    """Test initialization and configuration of HistoricalDataGenerator."""

    def test_init_with_valid_env_vars(self):
        """Test successful initialization with all required environment variables."""
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client") as mock_boto_client:
                generator = HistoricalDataGenerator()

                assert generator.endpoint_url == "http://localhost:9000"
                assert generator.access_key == "test_access_key"
                assert generator.secret_key == "test_secret_key"
                assert generator.bucket_name == "test-bucket"
                assert generator.data_generator is not None
                assert generator.s3_client is not None

                # Verify boto3 client was called with correct parameters
                mock_boto_client.assert_called_once_with(
                    "s3",
                    endpoint_url="http://localhost:9000",
                    aws_access_key_id="test_access_key",
                    aws_secret_access_key="test_secret_key",
                    region_name="eu-west-1",
                    use_ssl=False,
                    verify=False,
                )

    def test_init_missing_aws_endpoint(self):
        """Test initialization fails when AWS_ENDPOINT is missing."""
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
            clear=True,
        ):
            with pytest.raises(
                ValueError, match="AWS_ENDPOINT environment variable is required"
            ):
                HistoricalDataGenerator()

    def test_init_missing_access_key(self):
        """Test initialization fails when AWS_ACCESS_KEY_ID is missing."""
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
            clear=True,
        ):
            with pytest.raises(
                ValueError, match="AWS_ACCESS_KEY_ID environment variable is required"
            ):
                HistoricalDataGenerator()

    def test_init_missing_secret_key(self):
        """Test initialization fails when AWS_SECRET_ACCESS_KEY is missing."""
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
            clear=True,
        ):
            with pytest.raises(
                ValueError,
                match="AWS_SECRET_ACCESS_KEY environment variable is required",
            ):
                HistoricalDataGenerator()

    def test_init_missing_bucket_name(self):
        """Test initialization fails when AWS_S3_BUCKET_DATA is missing."""
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
            },
            clear=True,
        ):
            with pytest.raises(
                ValueError, match="AWS_S3_BUCKET_DATA environment variable is required"
            ):
                HistoricalDataGenerator()


class TestHistoricalEventGeneration:
    """Test historical event generation functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client"):
                self.generator = HistoricalDataGenerator()

    def test_generate_historical_events_basic(self):
        """Test basic historical event generation."""
        start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 1, 1, 2, 0, 0, tzinfo=timezone.utc)  # 2 hours
        events_per_hour = 10

        with patch.object(
            self.generator.data_generator, "generate_event"
        ) as mock_generate:
            # Create a mock event
            mock_event = Mock(spec=IoTEvent)
            mock_event.timestamp = start_date
            mock_generate.return_value = mock_event

            events = self.generator.generate_historical_events(
                start_date, end_date, events_per_hour
            )

            # Should generate 1 hour * 10 events per hour = 10 events (the loop logic generates for 1 hour)
            assert len(events) == 10
            assert mock_generate.call_count == 10

            # Check that all events have timestamps set
            for event in events:
                assert hasattr(event, "timestamp")

    def test_generate_historical_events_timestamp_progression(self):
        """Test that timestamps progress correctly through the time range."""
        start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)  # 1 hour
        events_per_hour = 60  # 1 event per minute

        with patch.object(
            self.generator.data_generator, "generate_event"
        ) as mock_generate:
            mock_event = Mock(spec=IoTEvent)
            mock_generate.return_value = mock_event

            events = self.generator.generate_historical_events(
                start_date, end_date, events_per_hour
            )

            assert len(events) == 60

            # Check that timestamps are within the expected range
            timestamps = [event.timestamp for event in events]
            assert all(start_date <= ts <= end_date for ts in timestamps)

    def test_generate_historical_events_empty_range(self):
        """Test generation with empty time range."""
        start_date = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)  # Same time

        events = self.generator.generate_historical_events(start_date, end_date, 10)
        assert len(events) == 0

    def test_generate_historical_events_zero_events_per_hour(self):
        """Test generation with zero events per hour."""
        start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

        events = self.generator.generate_historical_events(start_date, end_date, 0)
        assert len(events) == 0


class TestEventGrouping:
    """Test event grouping functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client"):
                self.generator = HistoricalDataGenerator()

    def test_group_events_by_hour_single_hour(self):
        """Test grouping events from a single hour."""
        # Create mock events for the same hour
        timestamp = datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        events = []
        for i in range(5):
            event = Mock(spec=IoTEvent)
            event.timestamp = timestamp + timedelta(minutes=i)
            events.append(event)

        grouped = self.generator.group_events_by_hour(events)

        assert len(grouped) == 1
        assert "2025/01/01/10" in grouped
        assert len(grouped["2025/01/01/10"]) == 5

    def test_group_events_by_hour_multiple_hours(self):
        """Test grouping events from multiple hours."""
        events = []

        # Create events for 3 different hours
        for hour in [10, 11, 12]:
            for minute in [0, 30]:
                event = Mock(spec=IoTEvent)
                event.timestamp = datetime(
                    2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc
                )
                events.append(event)

        grouped = self.generator.group_events_by_hour(events)

        assert len(grouped) == 3
        assert "2025/01/01/10" in grouped
        assert "2025/01/01/11" in grouped
        assert "2025/01/01/12" in grouped
        assert len(grouped["2025/01/01/10"]) == 2
        assert len(grouped["2025/01/01/11"]) == 2
        assert len(grouped["2025/01/01/12"]) == 2

    def test_group_events_by_hour_empty_list(self):
        """Test grouping empty event list."""
        grouped = self.generator.group_events_by_hour([])
        assert len(grouped) == 0

    def test_group_events_by_hour_cross_day_boundary(self):
        """Test grouping events that cross day boundaries."""
        events = []

        # Events at 23:30 on day 1 and 00:30 on day 2
        event1 = Mock(spec=IoTEvent)
        event1.timestamp = datetime(2025, 1, 1, 23, 30, 0, tzinfo=timezone.utc)
        events.append(event1)

        event2 = Mock(spec=IoTEvent)
        event2.timestamp = datetime(2025, 1, 2, 0, 30, 0, tzinfo=timezone.utc)
        events.append(event2)

        grouped = self.generator.group_events_by_hour(events)

        assert len(grouped) == 2
        assert "2025/01/01/23" in grouped
        assert "2025/01/02/00" in grouped
        assert len(grouped["2025/01/01/23"]) == 1
        assert len(grouped["2025/01/02/00"]) == 1


class TestS3Upload:
    """Test S3 upload functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client") as mock_boto_client:
                self.mock_s3_client = Mock()
                mock_boto_client.return_value = self.mock_s3_client
                self.generator = HistoricalDataGenerator()

    def test_upload_hourly_batch_success(self):
        """Test successful upload of hourly batch."""
        # Create mock events
        events = []
        for i in range(3):
            event = Mock(spec=IoTEvent)
            event.model_dump.return_value = {
                "device_id": f"device_{i}",
                "device_type": "temperature_sensor",
                "value": 20.0 + i,
                "event_duration": 1.0,
            }
            event.timestamp = datetime(2025, 1, 1, 10, i, 0, tzinfo=timezone.utc)
            events.append(event)

        hour_key = "2025/01/01/10"
        result = self.generator.upload_hourly_batch(hour_key, events)

        assert result is True
        self.mock_s3_client.put_object.assert_called_once()

        # Check the call arguments
        call_args = self.mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Key"] == "historical/raw/2025/01/01/10/events.json"
        assert call_args[1]["ContentType"] == "application/json"

        # Verify the JSON content
        uploaded_data = json.loads(call_args[1]["Body"])
        assert len(uploaded_data) == 3
        assert all("timestamp" in event for event in uploaded_data)

    def test_upload_hourly_batch_s3_error(self):
        """Test upload failure due to S3 error."""
        self.mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
            "PutObject",
        )

        events = [Mock(spec=IoTEvent)]
        events[0].model_dump.return_value = {"device_id": "test"}
        events[0].timestamp = datetime.now(timezone.utc)

        result = self.generator.upload_hourly_batch("2025/01/01/10", events)
        assert result is False

    def test_upload_hourly_batch_empty_events(self):
        """Test upload with empty events list."""
        result = self.generator.upload_hourly_batch("2025/01/01/10", [])

        assert result is True
        self.mock_s3_client.put_object.assert_called_once()

        # Check that empty list was uploaded
        call_args = self.mock_s3_client.put_object.call_args
        uploaded_data = json.loads(call_args[1]["Body"])
        assert len(uploaded_data) == 0


class TestAggregatedData:
    """Test aggregated data creation and upload."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client") as mock_boto_client:
                self.mock_s3_client = Mock()
                mock_boto_client.return_value = self.mock_s3_client
                self.generator = HistoricalDataGenerator()

    def test_create_aggregated_data_single_device(self):
        """Test aggregation for a single device."""
        # Create mock events for the same device and hour
        events = []
        timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        for i in range(3):
            event = Mock(spec=IoTEvent)
            event.timestamp = timestamp + timedelta(minutes=i * 10)
            event.device_id = "temp_001"
            event.device_type = "temperature_sensor"
            event.location = "Living Room"
            event.unit = "°C"
            event.value = 20.0 + i
            event.event_duration = 1.0 + i * 0.1
            event.battery_level = 90 - i
            event.signal_strength = -50 - i
            events.append(event)

        aggregations = self.generator.create_aggregated_data(events)

        assert len(aggregations) == 1
        agg = aggregations[0]

        assert agg["device_id"] == "temp_001"
        assert agg["device_type"] == "temperature_sensor"
        assert agg["location"] == "Living Room"
        assert agg["unit"] == "°C"
        assert agg["event_count"] == 3
        assert agg["avg_value"] == 21.0  # (20 + 21 + 22) / 3
        assert agg["min_value"] == 20.0
        assert agg["max_value"] == 22.0
        assert abs(agg["avg_event_duration"] - 1.1) < 0.001  # (1.0 + 1.1 + 1.2) / 3
        assert agg["avg_battery_level"] == 89.0  # (90 + 89 + 88) / 3
        assert agg["avg_signal_strength"] == -51.0  # (-50 + -51 + -52) / 3
        assert agg["year"] == 2025
        assert agg["month"] == 1
        assert agg["day"] == 1
        assert agg["hour"] == 10

    def test_create_aggregated_data_multiple_devices(self):
        """Test aggregation for multiple devices."""
        events = []
        timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        # Create events for two different devices
        for device_id in ["temp_001", "temp_002"]:
            for i in range(2):
                event = Mock(spec=IoTEvent)
                event.timestamp = timestamp + timedelta(minutes=i * 10)
                event.device_id = device_id
                event.device_type = "temperature_sensor"
                event.location = "Living Room"
                event.unit = "°C"
                event.value = 20.0 + i
                event.event_duration = 1.0
                event.battery_level = 90
                event.signal_strength = -50
                events.append(event)

        aggregations = self.generator.create_aggregated_data(events)

        assert len(aggregations) == 2
        device_ids = [agg["device_id"] for agg in aggregations]
        assert "temp_001" in device_ids
        assert "temp_002" in device_ids

    def test_create_aggregated_data_none_values(self):
        """Test aggregation with None battery and signal values."""
        events = []
        timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        event = Mock(spec=IoTEvent)
        event.timestamp = timestamp
        event.device_id = "temp_001"
        event.device_type = "temperature_sensor"
        event.location = "Living Room"
        event.unit = "°C"
        event.value = 20.0
        event.event_duration = 1.0
        event.battery_level = None
        event.signal_strength = None
        events.append(event)

        aggregations = self.generator.create_aggregated_data(events)

        assert len(aggregations) == 1
        agg = aggregations[0]
        assert agg["avg_battery_level"] is None
        assert agg["avg_signal_strength"] is None

    def test_upload_aggregated_batch_success(self):
        """Test successful upload of aggregated batch."""
        aggregations = [{"device_id": "temp_001", "avg_value": 21.0, "event_count": 3}]

        result = self.generator.upload_aggregated_batch("2025/01/01/10", aggregations)

        assert result is True
        self.mock_s3_client.put_object.assert_called_once()

        call_args = self.mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert (
            call_args[1]["Key"]
            == "historical/aggregated/2025/01/01/10/aggregations.json"
        )
        assert call_args[1]["ContentType"] == "application/json"

    def test_upload_aggregated_batch_error(self):
        """Test upload failure for aggregated batch."""
        self.mock_s3_client.put_object.side_effect = Exception("Upload failed")

        aggregations = [{"device_id": "temp_001"}]
        result = self.generator.upload_aggregated_batch("2025/01/01/10", aggregations)

        assert result is False


class TestWeekDataGeneration:
    """Test complete week data generation workflow."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client") as mock_boto_client:
                self.mock_s3_client = Mock()
                mock_boto_client.return_value = self.mock_s3_client
                self.generator = HistoricalDataGenerator()

    @patch("src.historical_data_generator.datetime")
    def test_generate_week_of_data_success(self, mock_datetime):
        """Test successful generation of a week of data."""
        # Mock current time
        mock_now = datetime(2025, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now

        # Mock the methods to avoid actual data generation and upload
        with patch.object(
            self.generator, "generate_historical_events"
        ) as mock_generate:
            with patch.object(self.generator, "group_events_by_hour") as mock_group:
                with patch.object(
                    self.generator, "upload_hourly_batch"
                ) as mock_upload_raw:
                    with patch.object(
                        self.generator, "create_aggregated_data"
                    ) as mock_create_agg:
                        with patch.object(
                            self.generator, "upload_aggregated_batch"
                        ) as mock_upload_agg:

                            # Setup mock returns
                            mock_events = [Mock(spec=IoTEvent) for _ in range(10)]
                            mock_generate.return_value = mock_events

                            mock_grouped = {
                                "2025/01/01/10": mock_events[:5],
                                "2025/01/01/11": mock_events[5:],
                            }
                            mock_group.return_value = mock_grouped

                            mock_upload_raw.return_value = True
                            mock_upload_agg.return_value = True
                            mock_create_agg.return_value = [{"device_id": "test"}]

                            result = self.generator.generate_week_of_data(7)

                            assert result is True
                            mock_generate.assert_called_once()
                            mock_group.assert_called_once_with(mock_events)
                            assert mock_upload_raw.call_count == 2
                            assert mock_upload_agg.call_count == 2

    @patch("src.historical_data_generator.datetime")
    def test_generate_week_of_data_upload_failure(self, mock_datetime):
        """Test handling of upload failures."""
        mock_now = datetime(2025, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now

        with patch.object(
            self.generator, "generate_historical_events"
        ) as mock_generate:
            with patch.object(self.generator, "group_events_by_hour") as mock_group:
                with patch.object(
                    self.generator, "upload_hourly_batch"
                ) as mock_upload_raw:
                    with patch.object(
                        self.generator, "create_aggregated_data"
                    ) as mock_create_agg:
                        with patch.object(
                            self.generator, "upload_aggregated_batch"
                        ) as mock_upload_agg:

                            mock_events = [Mock(spec=IoTEvent) for _ in range(5)]
                            mock_generate.return_value = mock_events
                            mock_group.return_value = {"2025/01/01/10": mock_events}
                            mock_upload_raw.return_value = False  # Simulate failure
                            mock_upload_agg.return_value = True
                            mock_create_agg.return_value = [{"device_id": "test"}]

                            result = self.generator.generate_week_of_data(1)

                            assert result is False

    def test_generate_week_of_data_custom_days(self):
        """Test generation with custom number of days."""
        with patch("src.historical_data_generator.datetime") as mock_datetime:
            mock_now = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now

            with patch.object(
                self.generator, "generate_historical_events"
            ) as mock_generate:
                with patch.object(self.generator, "group_events_by_hour") as mock_group:
                    with patch.object(
                        self.generator, "upload_hourly_batch"
                    ) as mock_upload_raw:
                        with patch.object(
                            self.generator, "create_aggregated_data"
                        ) as mock_create_agg:
                            with patch.object(
                                self.generator, "upload_aggregated_batch"
                            ) as mock_upload_agg:

                                mock_generate.return_value = []
                                mock_group.return_value = {}
                                mock_upload_raw.return_value = True
                                mock_upload_agg.return_value = True
                                mock_create_agg.return_value = []

                                self.generator.generate_week_of_data(3)

                                # Check that the date range is correct for 3 days
                                call_args = mock_generate.call_args[0]
                                start_date, end_date = call_args[0], call_args[1]

                                expected_start = datetime(
                                    2025, 1, 12, 0, 0, 0, tzinfo=timezone.utc
                                )
                                expected_end = datetime(
                                    2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc
                                )

                                assert start_date == expected_start
                                assert end_date == expected_end


class TestMainFunction:
    """Test the main function and CLI integration."""

    @patch("src.historical_data_generator.HistoricalDataGenerator")
    @patch("src.historical_data_generator.exit")
    def test_main_success_default_days(self, mock_exit, mock_generator_class):
        """Test main function with default days setting."""
        mock_generator = Mock()
        mock_generator.generate_week_of_data.return_value = True
        mock_generator_class.return_value = mock_generator

        with patch.dict(os.environ, {}, clear=True):  # No HISTORICAL_DAYS set
            from src.historical_data_generator import main

            main()

            mock_generator.generate_week_of_data.assert_called_once_with(7)
            mock_exit.assert_called_once_with(0)

    @patch("src.historical_data_generator.HistoricalDataGenerator")
    @patch("src.historical_data_generator.exit")
    def test_main_success_custom_days(self, mock_exit, mock_generator_class):
        """Test main function with custom days setting."""
        mock_generator = Mock()
        mock_generator.generate_week_of_data.return_value = True
        mock_generator_class.return_value = mock_generator

        with patch.dict(os.environ, {"HISTORICAL_DAYS": "14"}):
            from src.historical_data_generator import main

            main()

            mock_generator.generate_week_of_data.assert_called_once_with(14)
            mock_exit.assert_called_once_with(0)

    @patch("src.historical_data_generator.HistoricalDataGenerator")
    @patch("src.historical_data_generator.exit")
    def test_main_failure(self, mock_exit, mock_generator_class):
        """Test main function when generation fails."""
        mock_generator = Mock()
        mock_generator.generate_week_of_data.return_value = False
        mock_generator_class.return_value = mock_generator

        with patch.dict(os.environ, {}):
            from src.historical_data_generator import main

            main()

            mock_exit.assert_called_once_with(1)

    def test_main_invalid_days_env_var(self):
        """Test main function with invalid HISTORICAL_DAYS environment variable."""
        with patch.dict(os.environ, {"HISTORICAL_DAYS": "invalid"}):
            with pytest.raises(
                ValueError, match="HISTORICAL_DAYS must be a valid integer"
            ):
                from src.historical_data_generator import main

                main()


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def setup_method(self):
        """Set up test fixtures."""
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message="The `__fields__` attribute is deprecated, use `model_fields` instead.",
        )
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT": "http://localhost:9000",
                "AWS_ACCESS_KEY_ID": "test_access_key",
                "AWS_SECRET_ACCESS_KEY": "test_secret_key",
                "AWS_S3_BUCKET_DATA": "test-bucket",
            },
        ):
            with patch("boto3.client") as mock_boto_client:
                self.mock_s3_client = Mock()
                mock_boto_client.return_value = self.mock_s3_client
                self.generator = HistoricalDataGenerator()

    def test_event_serialization_with_special_characters(self):
        """Test event serialization with special characters in data."""
        events = []
        event = Mock(spec=IoTEvent)
        event.model_dump.return_value = {
            "device_id": "temp_001",
            "location": 'Living Room "Main"',  # Special characters
            "metadata": {"note": 'Temperature sensor with "quotes"'},
        }
        event.timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        events.append(event)

        result = self.generator.upload_hourly_batch("2025/01/01/10", events)
        assert result is True

        # Verify JSON was properly serialized
        call_args = self.mock_s3_client.put_object.call_args
        uploaded_data = json.loads(call_args[1]["Body"])
        assert len(uploaded_data) == 1

    def test_large_batch_upload(self):
        """Test upload of large batch of events."""
        events = []
        for i in range(1000):  # Large number of events
            event = Mock(spec=IoTEvent)
            event.model_dump.return_value = {
                "device_id": f"device_{i}",
                "value": i * 0.1,
            }
            event.timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            events.append(event)

        result = self.generator.upload_hourly_batch("2025/01/01/10", events)
        assert result is True

        # Verify all events were included
        call_args = self.mock_s3_client.put_object.call_args
        uploaded_data = json.loads(call_args[1]["Body"])
        assert len(uploaded_data) == 1000

    def test_timezone_handling(self):
        """Test proper handling of different timezones."""
        events = []

        # Create event with UTC timezone
        event = Mock(spec=IoTEvent)
        event.timestamp = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        events.append(event)

        grouped = self.generator.group_events_by_hour(events)

        # Should be grouped by UTC time
        assert "2025/01/01/10" in grouped
        assert len(grouped["2025/01/01/10"]) == 1


if __name__ == "__main__":
    pytest.main([__file__])
