"""
Spark Structured Streaming ETL job for IoT events.
"""
import os
import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    count,
    max as spark_max,
    min as spark_min,
    stddev,
    when,
    isnan,
    current_timestamp,
    year,
    month,
    dayofmonth,
    hour,
)
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


class IoTStreamingETL:
    """Spark Structured Streaming ETL for IoT events."""

    def __init__(self):
        """Initialize the streaming ETL job."""

        # Configuration from environment - no defaults, raise errors if missing
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not self.kafka_bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        self.kafka_topic = os.getenv("KAFKA_TOPIC")
        if not self.kafka_topic:
            raise ValueError("KAFKA_TOPIC environment variable is required")

        self.checkpoint_location = os.getenv("CHECKPOINT_LOCATION")
        if not self.checkpoint_location:
            raise ValueError("CHECKPOINT_LOCATION environment variable is required")

        self.output_path = os.getenv("OUTPUT_PATH")
        if not self.output_path:
            raise ValueError("OUTPUT_PATH environment variable is required")

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        if not self.minio_endpoint:
            raise ValueError("MINIO_ENDPOINT environment variable is required")

        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        if not self.aws_access_key:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")

        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not self.aws_secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")

        self.spark = self._create_spark_session()
        self.schema = self._define_schema()

        logger.info("Initialized IoT Streaming ETL")
        logger.info(f"Kafka servers: {self.kafka_bootstrap_servers}")
        logger.info(f"Kafka topic: {self.kafka_topic}")
        logger.info(f"Output path: {self.output_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark = (
            SparkSession.builder.appName("IoTStreamingETL")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        return spark

    def _define_schema(self) -> StructType:
        """Define the schema for IoT events."""
        return StructType(
            [
                StructField("event_duration", DoubleType(), False),
                StructField("device_type", StringType(), False),
                StructField("device_id", StringType(), False),
                StructField(
                    "timestamp", StringType(), False
                ),  # Will convert to timestamp
                StructField("location", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("unit", StringType(), False),
                StructField("battery_level", IntegerType(), True),
                StructField("signal_strength", IntegerType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    def read_kafka_stream(self) -> DataFrame:
        """Read streaming data from Kafka."""
        logger.info("Setting up Kafka stream reader...")

        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.group.id", "iot-streaming-etl")
            .load()
        )

        logger.info("Kafka stream reader configured")
        return kafka_df

    def parse_json_data(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON data from Kafka messages."""
        logger.info("Parsing JSON data from Kafka messages...")

        # Parse JSON from Kafka value column
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), self.schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),
        ).select("data.*", "kafka_timestamp", "partition", "offset")

        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )

        # Add processing timestamp
        parsed_df = parsed_df.withColumn("processing_timestamp", current_timestamp())

        logger.info("JSON parsing configured")
        return parsed_df

    def apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply data quality checks and filtering."""
        logger.info("Applying data quality checks...")

        # Filter out records with null required fields
        quality_df = df.filter(
            col("event_duration").isNotNull()
            & col("device_type").isNotNull()
            & col("device_id").isNotNull()
            & col("timestamp").isNotNull()
            & col("value").isNotNull()
        )

        # Filter out invalid values
        quality_df = quality_df.filter(
            (col("event_duration") > 0)
            & (col("event_duration") <= 3600)
            & (~isnan(col("value")))  # Max 1 hour
            & (
                col("battery_level").isNull()
                | ((col("battery_level") >= 0) & (col("battery_level") <= 100))
            )
            & (
                col("signal_strength").isNull()
                | ((col("signal_strength") >= -120) & (col("signal_strength") <= -30))
            )
        )

        # Add data quality flags
        quality_df = quality_df.withColumn(
            "is_valid_battery",
            when(col("battery_level").isNull(), True).otherwise(
                (col("battery_level") >= 20) & (col("battery_level") <= 100)
            ),
        ).withColumn(
            "is_valid_signal",
            when(col("signal_strength").isNull(), True).otherwise(
                col("signal_strength") >= -90
            ),
        )

        logger.info("Data quality checks applied")
        return quality_df

    def create_windowed_aggregations(self, df: DataFrame) -> DataFrame:
        """Create windowed aggregations (average value per device per minute)."""
        logger.info("Creating windowed aggregations...")

        # Create 1-minute windows
        windowed_df = (
            df.withWatermark("timestamp", "2 minutes")
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("device_id"),
                col("device_type"),
                col("location"),
                col("unit"),
            )
            .agg(
                avg("value").alias("avg_value"),
                count("value").alias("event_count"),
                spark_min("value").alias("min_value"),
                spark_max("value").alias("max_value"),
                stddev("value").alias("stddev_value"),
                avg("event_duration").alias("avg_event_duration"),
                avg("battery_level").alias("avg_battery_level"),
                avg("signal_strength").alias("avg_signal_strength"),
                count(when(col("is_valid_battery") is False, 1)).alias(
                    "low_battery_count"
                ),
                count(when(col("is_valid_signal") is False, 1)).alias(
                    "weak_signal_count"
                ),
            )
        )

        # Add window start and end times as separate columns
        windowed_df = windowed_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("device_type"),
            col("location"),
            col("unit"),
            col("avg_value"),
            col("event_count"),
            col("min_value"),
            col("max_value"),
            col("stddev_value"),
            col("avg_event_duration"),
            col("avg_battery_level"),
            col("avg_signal_strength"),
            col("low_battery_count"),
            col("weak_signal_count"),
        )

        # Add partitioning columns for efficient storage
        windowed_df = (
            windowed_df.withColumn("year", year(col("window_start")))
            .withColumn("month", month(col("window_start")))
            .withColumn("day", dayofmonth(col("window_start")))
            .withColumn("hour", hour(col("window_start")))
        )

        logger.info("Windowed aggregations created")
        return windowed_df

    def write_to_storage(
        self, df: DataFrame, output_path: str, checkpoint_path: str
    ) -> None:
        """Write processed data to MinIO/S3 in Parquet format."""
        logger.info(f"Writing to storage: {output_path}")

        query = (
            df.writeStream.format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("year", "month", "day", "hour")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info(f"Streaming query started with checkpoint: {checkpoint_path}")
        return query

    def write_raw_data(self, df: DataFrame) -> None:
        """Write raw parsed data to storage for backup."""
        logger.info("Setting up raw data writer...")

        # Add partitioning columns
        raw_df = (
            df.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
        )

        raw_output_path = f"{self.output_path}/raw/"
        raw_checkpoint_path = f"{self.checkpoint_location}/raw/"

        raw_query = self.write_to_storage(raw_df, raw_output_path, raw_checkpoint_path)
        return raw_query

    def write_aggregated_data(self, df: DataFrame) -> None:
        """Write aggregated data to storage."""
        logger.info("Setting up aggregated data writer...")

        agg_output_path = f"{self.output_path}/aggregated/"
        agg_checkpoint_path = f"{self.checkpoint_location}/aggregated/"

        agg_query = self.write_to_storage(df, agg_output_path, agg_checkpoint_path)
        return agg_query

    def run_streaming_job(self):
        """Run the complete streaming ETL job."""
        logger.info("Starting IoT Streaming ETL job...")

        try:
            # Read from Kafka
            kafka_stream = self.read_kafka_stream()

            # Parse JSON data
            parsed_stream = self.parse_json_data(kafka_stream)

            # Apply data quality checks
            quality_stream = self.apply_data_quality_checks(parsed_stream)

            # Create windowed aggregations
            aggregated_stream = self.create_windowed_aggregations(quality_stream)

            # Start writing streams
            raw_query = self.write_raw_data(quality_stream)
            agg_query = self.write_aggregated_data(aggregated_stream)

            logger.info("Streaming queries started successfully")
            logger.info("Press Ctrl+C to stop the streaming job")

            # Wait for termination
            raw_query.awaitTermination()
            agg_query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in streaming job: {e}")
            raise
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()


def main():
    """Main function to run the streaming ETL job."""
    logger.info("Starting IoT Streaming ETL application")

    etl = IoTStreamingETL()
    etl.run_streaming_job()


if __name__ == "__main__":
    main()
