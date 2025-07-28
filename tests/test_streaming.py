"""
Unit tests for Spark streaming ETL.
"""
import pytest
import os
from unittest.mock import Mock, patch
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestIoTStreamingETL:
    """Test cases for IoT streaming ETL."""

    def test_environment_variable_validation(self):
        """Test that missing environment variables raise appropriate errors."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            from src.streaming.streaming_etl import IoTStreamingETL

            # Test missing KAFKA_BOOTSTRAP_SERVERS
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(
                    ValueError,
                    match="KAFKA_BOOTSTRAP_SERVERS environment variable is required",
                ):
                    IoTStreamingETL()

            # Test missing KAFKA_TOPIC
            with patch.dict(
                os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "kafka:29092"}, clear=True
            ):
                with pytest.raises(
                    ValueError, match="KAFKA_TOPIC environment variable is required"
                ):
                    IoTStreamingETL()

            # Test missing CHECKPOINT_LOCATION
            with patch.dict(
                os.environ,
                {"KAFKA_BOOTSTRAP_SERVERS": "kafka:29092", "KAFKA_TOPIC": "iot-events"},
                clear=True,
            ):
                with pytest.raises(
                    ValueError,
                    match="CHECKPOINT_LOCATION environment variable is required",
                ):
                    IoTStreamingETL()

            # Test missing OUTPUT_PATH
            with patch.dict(
                os.environ,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                    "KAFKA_TOPIC": "iot-events",
                    "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                },
                clear=True,
            ):
                with pytest.raises(
                    ValueError, match="OUTPUT_PATH environment variable is required"
                ):
                    IoTStreamingETL()

            # Test missing AWS_ENDPOINT
            with patch.dict(
                os.environ,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                    "KAFKA_TOPIC": "iot-events",
                    "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                    "OUTPUT_PATH": "s3a://bucket/path",
                },
                clear=True,
            ):
                with pytest.raises(
                    ValueError, match="AWS_ENDPOINT environment variable is required"
                ):
                    IoTStreamingETL()

            # Test missing AWS_ACCESS_KEY_ID
            with patch.dict(
                os.environ,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                    "KAFKA_TOPIC": "iot-events",
                    "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                    "OUTPUT_PATH": "s3a://bucket/path",
                    "AWS_ENDPOINT": "http://localhost:9000",
                },
                clear=True,
            ):
                with pytest.raises(
                    ValueError,
                    match="AWS_ACCESS_KEY_ID environment variable is required",
                ):
                    IoTStreamingETL()

            # Test missing AWS_SECRET_ACCESS_KEY
            with patch.dict(
                os.environ,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                    "KAFKA_TOPIC": "iot-events",
                    "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                    "OUTPUT_PATH": "s3a://bucket/path",
                    "AWS_ENDPOINT": "http://localhost:9000",
                    "AWS_ACCESS_KEY_ID": "minioadmin",
                },
                clear=True,
            ):
                with pytest.raises(
                    ValueError,
                    match="AWS_SECRET_ACCESS_KEY environment variable is required",
                ):
                    IoTStreamingETL()

    def test_successful_initialization(self):
        """Test successful initialization with all required environment variables."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create a mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder

                # Chain the builder methods
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    assert etl.kafka_bootstrap_servers == "kafka:29092"
                    assert etl.kafka_topic == "iot-events"
                    assert etl.checkpoint_location == "/tmp/checkpoints"
                    assert etl.output_path == "s3a://bucket/path"
                    assert etl.aws_endpoint == "http://localhost:9000"
                    assert etl.aws_access_key == "minioadmin"
                    assert etl.aws_secret_key == "minioadmin"

    def test_schema_definition(self):
        """Test schema definition."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create a mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder

                # Chain the builder methods
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    assert hasattr(etl, "_define_schema")
                    assert callable(getattr(etl, "_define_schema"))

                    schema = etl._define_schema()
                    assert schema is not None

    def test_kafka_stream_configuration(self):
        """Test Kafka stream reader configuration."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create mock spark session and components
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create mock read stream chain
                mock_read_stream = Mock()
                mock_spark.readStream = mock_read_stream

                # Create mock for the fluent interface chain
                mock_format_result = Mock()
                mock_read_stream.format.return_value = mock_format_result

                # Chain the option calls
                mock_format_result.option.return_value = mock_format_result
                mock_format_result.load.return_value = Mock()  # Final DataFrame mock

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder

                # Chain the builder methods
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()
                    _ = etl.read_kafka_stream()

                    # Verify Kafka configuration
                    mock_read_stream.format.assert_called_once_with("kafka")

                    # Verify that option was called multiple times
                    assert mock_format_result.option.call_count >= 5

                    # Check that load was called
                    mock_format_result.load.assert_called_once()

    def test_method_calls_without_execution(self):
        """Test that methods can be called without executing PySpark functions."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    # Test that we can create the ETL object without errors
                    assert etl is not None
                    assert hasattr(etl, "kafka_bootstrap_servers")
                    assert hasattr(etl, "kafka_topic")
                    assert hasattr(etl, "checkpoint_location")
                    assert hasattr(etl, "output_path")


class TestDataQualityChecks:
    """Test data quality validation functions."""

    def test_data_quality_method_exists(self):
        """Test that data quality method exists and can be called."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    # Test that the method exists
                    assert hasattr(etl, "apply_data_quality_checks")
                    assert callable(getattr(etl, "apply_data_quality_checks"))


class TestWindowedAggregations:
    """Test windowed aggregation logic."""

    def test_windowed_aggregation_method_exists(self):
        """Test that windowed aggregation method exists."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    # Test that the method exists
                    assert hasattr(etl, "create_windowed_aggregations")
                    assert callable(getattr(etl, "create_windowed_aggregations"))


class TestStreamWriting:
    """Test stream writing functionality."""

    def test_write_methods_exist(self):
        """Test that write methods exist."""
        # Mock PySpark modules before importing
        with patch.dict(
            "sys.modules",
            {
                "pyspark": Mock(),
                "pyspark.sql": Mock(),
                "pyspark.sql.functions": Mock(),
                "pyspark.sql.types": Mock(),
                "dotenv": Mock(),
            },
        ):
            with patch(
                "src.streaming.streaming_etl.SparkSession"
            ) as mock_spark_session:
                from src.streaming.streaming_etl import IoTStreamingETL

                # Create mock spark session
                mock_spark = Mock()
                mock_spark.sparkContext.setLogLevel = Mock()

                # Create a mock builder chain
                mock_builder = Mock()
                mock_spark_session.builder = mock_builder
                mock_builder.appName.return_value = mock_builder
                mock_builder.config.return_value = mock_builder
                mock_builder.getOrCreate.return_value = mock_spark

                with patch.dict(
                    os.environ,
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
                        "KAFKA_TOPIC": "iot-events",
                        "CHECKPOINT_LOCATION": "/tmp/checkpoints",
                        "OUTPUT_PATH": "s3a://bucket/path",
                        "AWS_ENDPOINT": "http://localhost:9000",
                        "AWS_ACCESS_KEY_ID": "minioadmin",
                        "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    },
                ):
                    etl = IoTStreamingETL()

                    # Test that write methods exist
                    assert hasattr(etl, "write_to_storage")
                    assert callable(getattr(etl, "write_to_storage"))
                    assert hasattr(etl, "write_raw_data")
                    assert callable(getattr(etl, "write_raw_data"))
                    assert hasattr(etl, "write_aggregated_data")
                    assert callable(getattr(etl, "write_aggregated_data"))


if __name__ == "__main__":
    pytest.main([__file__])
