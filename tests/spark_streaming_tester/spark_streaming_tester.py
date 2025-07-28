"""
Spark Streaming Kafka consumer test script.
Tests connectivity to Kafka and consumes IoT events with timeout functionality.
"""
import os
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    MapType,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SparkStreamingTester:
    """Test Spark Streaming Kafka connectivity and message consumption."""

    def __init__(self):
        """Initialize the Spark streaming tester."""
        # Configuration from environment - no defaults, raise errors if missing
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not self.kafka_bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        self.kafka_topic = os.getenv("KAFKA_TOPIC")
        if not self.kafka_topic:
            raise ValueError("KAFKA_TOPIC environment variable is required")

        # Test-specific configuration with defaults
        self.consumer_timeout = int(
            os.getenv("CONSUMER_TIMEOUT", "60")
        )  # 60 seconds default
        self.producer_timeout = int(
            os.getenv("PRODUCER_TIMEOUT", "30")
        )  # 30 seconds default
        self.max_messages = int(os.getenv("MAX_MESSAGES", "10"))  # 10 messages default
        self.test_duration = int(os.getenv("TEST_DURATION", "30"))  # 30 seconds default

        self.spark = None
        self.schema = self._define_schema()
        self.messages_consumed = 0
        self.start_time = None
        self.stop_requested = False

        logger.info("Initialized Spark Streaming Tester")
        logger.info(f"Kafka servers: {self.kafka_bootstrap_servers}")
        logger.info(f"Kafka topic: {self.kafka_topic}")
        logger.info(f"Consumer timeout: {self.consumer_timeout}s")
        logger.info(f"Producer timeout: {self.producer_timeout}s")
        logger.info(f"Max messages: {self.max_messages}")
        logger.info(f"Test duration: {self.test_duration}s")

    def _define_schema(self) -> StructType:
        """Define the schema for IoT events."""
        return StructType(
            [
                StructField("event_duration", DoubleType(), False),
                StructField("device_type", StringType(), False),
                StructField("device_id", StringType(), False),
                StructField("timestamp", StringType(), False),
                StructField("location", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("unit", StringType(), False),
                StructField("battery_level", IntegerType(), True),
                StructField("signal_strength", IntegerType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for testing."""
        logger.info("Creating Spark session...")

        spark = (
            SparkSession.builder.appName("SparkStreamingTester")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/test-checkpoints")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark session created successfully")
        return spark

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, requesting stop...")
            self.stop_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def test_kafka_connectivity(self) -> bool:
        """Test basic Kafka connectivity."""
        try:
            logger.info("Testing Kafka connectivity...")

            # Create a simple Kafka reader to test connectivity
            _ = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("kafka.consumer.group.id", "spark-streaming-tester")
                .load()
            )

            logger.info("Kafka connectivity test successful")
            return True

        except Exception as e:
            logger.error(f"Kafka connectivity test failed: {e}")
            return False

    def consume_messages(self) -> bool:
        """Consume messages from Kafka and log them to stdout."""
        try:
            logger.info("Starting message consumption...")
            self.start_time = datetime.now(timezone.utc)

            # Read from Kafka
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("kafka.consumer.group.id", "spark-streaming-tester")
                .load()
            )

            # Parse JSON data
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), self.schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
            ).select("data.*", "kafka_timestamp", "partition", "offset")

            # Add processing timestamp
            parsed_df = parsed_df.withColumn(
                "processing_timestamp", current_timestamp()
            )

            # Start the streaming query with console output
            query = (
                parsed_df.writeStream.outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("numRows", "20")
                .trigger(processingTime="2 seconds")
                .start()
            )

            logger.info("Streaming query started, consuming messages...")

            # Monitor the query with timeout
            timeout_start = time.time()
            while query.isActive and not self.stop_requested:
                # Check timeout
                elapsed_time = time.time() - timeout_start
                if elapsed_time > self.test_duration:
                    logger.info(
                        f"Test duration ({self.test_duration}s) reached, stopping..."
                    )
                    break

                # Check query status
                if query.exception():
                    logger.error(f"Query failed with exception: {query.exception()}")
                    break

                # Get progress information
                progress = query.lastProgress
                if progress:
                    input_rows = progress.get("inputRowsPerSecond", 0)
                    processed_rows = progress.get("batchId", 0)
                    logger.info(
                        f"Progress - Batch: {processed_rows}, Input rate: {input_rows} rows/sec"
                    )

                time.sleep(2)

            # Stop the query gracefully
            logger.info("Stopping streaming query...")
            query.stop()

            # Wait for query to stop
            timeout = 10  # 10 seconds timeout for stopping
            stop_start = time.time()
            while query.isActive and (time.time() - stop_start) < timeout:
                time.sleep(0.5)

            if query.isActive:
                logger.warning("Query did not stop gracefully within timeout")
                return False

            logger.info("Message consumption completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            return False

    def run_comprehensive_test(self) -> bool:
        """Run comprehensive Spark streaming tests."""
        logger.info("Starting comprehensive Spark streaming tests...")

        success = True

        try:
            # Create Spark session
            self.spark = self._create_spark_session()

            # Setup signal handlers
            self._setup_signal_handlers()

            # Test 1: Kafka connectivity
            logger.info("=== Test 1: Kafka Connectivity ===")
            if not self.test_kafka_connectivity():
                success = False
                logger.error("Kafka connectivity test failed")
            else:
                logger.info("✓ Kafka connectivity test passed")

            # Test 2: Message consumption
            logger.info("=== Test 2: Message Consumption ===")
            if not self.consume_messages():
                success = False
                logger.error("Message consumption test failed")
            else:
                logger.info("✓ Message consumption test passed")

            # Test summary
            end_time = datetime.now(timezone.utc)
            if self.start_time:
                total_duration = (end_time - self.start_time).total_seconds()
                logger.info(f"Total test duration: {total_duration:.2f} seconds")

            if success:
                logger.info("🎉 All Spark streaming tests passed successfully!")
            else:
                logger.error("❌ Some Spark streaming tests failed!")

        except Exception as e:
            logger.error(f"Error during comprehensive test: {e}")
            success = False

        finally:
            # Clean up Spark session
            if self.spark:
                logger.info("Stopping Spark session...")
                self.spark.stop()

        return success

    def wait_for_producer(self) -> bool:
        """Wait for the producer to be ready and producing messages."""
        logger.info(
            f"Waiting for producer to be ready (timeout: {self.producer_timeout}s)..."
        )

        start_time = time.time()
        while (time.time() - start_time) < self.producer_timeout:
            try:
                # Try to create a simple Kafka consumer to check if messages are available
                temp_spark = (
                    SparkSession.builder.appName("ProducerWaiter")
                    .config(
                        "spark.sql.streaming.checkpointLocation",
                        "/tmp/temp-checkpoints",
                    )
                    .config(
                        "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
                    )
                    .getOrCreate()
                )

                temp_spark.sparkContext.setLogLevel("ERROR")

                # Try to read from Kafka
                kafka_df = (
                    temp_spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                    .option("subscribe", self.kafka_topic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load()
                )

                # Start a temporary query to check for data
                query = (
                    kafka_df.writeStream.outputMode("append")
                    .format("memory")
                    .queryName("temp_check")
                    .start()
                )

                # Wait a bit and check if we got any data
                time.sleep(3)

                if query.isActive:
                    progress = query.lastProgress
                    if progress and progress.get("inputRowsPerSecond", 0) > 0:
                        logger.info("✓ Producer is active and sending messages")
                        query.stop()
                        temp_spark.stop()
                        return True

                query.stop()
                temp_spark.stop()

            except Exception as e:
                logger.debug(f"Producer check attempt failed: {e}")

            logger.info("Producer not ready yet, waiting...")
            time.sleep(5)

        logger.warning(f"Producer timeout ({self.producer_timeout}s) reached")
        return False


def main():
    """Main function to run Spark streaming tests."""
    logger.info("Starting Spark Streaming Kafka Consumer Test...")

    try:
        tester = SparkStreamingTester()

        # Wait for producer to be ready
        if not tester.wait_for_producer():
            logger.warning("Producer is not ready, but continuing with tests...")

        # Run comprehensive tests
        success = tester.run_comprehensive_test()

        if success:
            logger.info("✅ Spark streaming testing completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Spark streaming testing failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
