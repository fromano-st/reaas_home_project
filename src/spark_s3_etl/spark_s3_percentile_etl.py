"""
Spark ETL job for computing 95th percentile of event_duration per device type per day.

This script reads IoT event data from S3/MinIO, performs outlier detection and filtering,
and computes the 95th percentile of event_duration per device type per day.

Query Requirements:
- Compute 95th percentile of event_duration per device type per day
- Exclude outliers that fall outside 3 standard deviations from the daily mean
- Only include device types that had at least 500 distinct events per day
"""

import os
import logging
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    mean,
    stddev,
    when,
    to_date,
    percentile_approx,
    current_timestamp,
    lit,
    round as spark_round,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SparkS3PercentileETL:
    """Spark ETL job for computing 95th percentile with outlier filtering."""

    def __init__(self):
        """
        Initialize the Spark ETL job.
        """
        self.bucket_name = os.getenv("AWS_S3_BUCKET_DATA")
        self.input_path = f"{self.bucket_name }/aggregated/"
        self.output_path = f"{self.bucket_name }/spark_results/"

        # S3/MinIO Configuration
        self.aws_endpoint = os.getenv("AWS_ENDPOINT")
        if not self.aws_endpoint:
            raise ValueError("AWS_ENDPOINT environment variable is required")

        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        if not self.aws_access_key:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")

        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not self.aws_secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")

        self.spark = self._create_spark_session()

        logger.info("Initialized Spark S3 Percentile ETL")
        logger.info(f"Input path: {self.input_path}")
        logger.info(f"Output path: {self.output_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with S3/MinIO support."""
        spark = (
            SparkSession.builder.appName("IoT_S3_Percentile_ETL")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            # Memory and performance tuning
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "2")
            .config("spark.executor.instances", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # Shuffle optimization
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.unsafe", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            # S3/MinIO configuration
            .config("spark.hadoop.fs.s3a.endpoint", self.aws_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # S3 performance tuning
            .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "40000")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
            .config("spark.hadoop.fs.s3a.retry.limit", "5")
            .config("spark.hadoop.fs.s3a.retry.interval", "500ms")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB
            .config("spark.hadoop.fs.s3a.readahead.range", "1048576")  # 1MB
            .getOrCreate()
        )

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        return spark

    def read_s3_data(self, date_filter: Optional[str] = None) -> DataFrame:
        """
        Read IoT event data from S3/MinIO.

        Args:
            date_filter: Optional date filter in YYYY-MM-DD format

        Returns:
            DataFrame with IoT event data
        """
        logger.info(f"Reading data from S3 path: {self.input_path}")

        try:
            # Read parquet files from S3
            df = self.spark.read.parquet(self.input_path)

            logger.info(
                f"Successfully read data from S3. Initial record count: {df.count()}"
            )

            # Apply date filter if provided
            if date_filter:
                df = df.filter(col("timestamp").cast("date") == date_filter)
                logger.info(
                    f"Applied date filter: {date_filter}. Filtered record count: {df.count()}"
                )

            # Show schema for debugging
            logger.info("Data schema:")
            df.printSchema()

            return df

        except Exception as e:
            logger.error(f"Error reading data from S3: {e}")
            raise

    def prepare_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare and clean the data for analysis.

        Args:
            df: Raw DataFrame from S3

        Returns:
            Cleaned DataFrame ready for analysis
        """
        logger.info("Preparing and cleaning data...")

        # Convert timestamp to date for daily aggregation
        df_prepared = df.withColumn("event_date", to_date(col("timestamp")))

        # Filter out invalid records
        df_prepared = df_prepared.filter(
            col("event_duration").isNotNull()
            & col("device_type").isNotNull()
            & col("event_date").isNotNull()
            & (col("event_duration") > 0)
            & (col("event_duration") <= 3600)  # Max 1 hour
        )

        logger.info(f"Data after cleaning: {df_prepared.count()} records")

        return df_prepared

    def detect_and_remove_outliers(self, df: DataFrame) -> DataFrame:
        """
        Detect and remove outliers that fall outside 3 standard deviations
        from the daily mean per device type.

        Args:
            df: Prepared DataFrame

        Returns:
            DataFrame with outliers removed
        """
        logger.info("Detecting and removing outliers (3 standard deviations)...")

        # Calculate daily statistics per device type
        daily_stats = df.groupBy("device_type", "event_date").agg(
            mean("event_duration").alias("daily_mean"),
            stddev("event_duration").alias("daily_stddev"),
            count("event_duration").alias("daily_count"),
        )

        # Join back with original data
        df_with_stats = df.join(
            daily_stats, on=["device_type", "event_date"], how="inner"
        )

        # Calculate outlier bounds (mean ± 3 * stddev)
        df_with_bounds = df_with_stats.withColumn(
            "lower_bound", col("daily_mean") - (3 * col("daily_stddev"))
        ).withColumn("upper_bound", col("daily_mean") + (3 * col("daily_stddev")))

        # Mark outliers
        df_with_outliers = df_with_bounds.withColumn(
            "is_outlier",
            when(
                (col("event_duration") < col("lower_bound"))
                | (col("event_duration") > col("upper_bound")),
                True,
            ).otherwise(False),
        )

        # Count outliers for logging
        outlier_count = df_with_outliers.filter(col("is_outlier") is True).count()
        total_count = df_with_outliers.count()

        logger.info(
            f"Detected {outlier_count} outliers out of {total_count} records ({outlier_count/total_count*100:.2f}%)"
        )

        # Remove outliers
        df_clean = df_with_outliers.filter(col("is_outlier") is False)

        logger.info(f"Data after outlier removal: {df_clean.count()} records")

        return df_clean

    def filter_device_types_by_event_count(self, df: DataFrame) -> DataFrame:
        """
        Filter to only include device types that had at least 500 distinct events per day.

        Args:
            df: DataFrame with outliers removed

        Returns:
            DataFrame filtered by minimum event count
        """
        logger.info(
            "Filtering device types with at least 500 distinct events per day..."
        )

        # Count distinct events per device type per day
        daily_event_counts = df.groupBy("device_type", "event_date").agg(
            countDistinct("device_id", "timestamp").alias("distinct_events")
        )

        # Filter device types with at least 500 distinct events
        valid_device_types = daily_event_counts.filter(
            col("distinct_events") >= 500
        ).select("device_type", "event_date")

        # Log the filtering results
        total_device_type_days = daily_event_counts.count()
        valid_device_type_days = valid_device_types.count()

        logger.info(
            f"Device type-day combinations before filtering: {total_device_type_days}"
        )
        logger.info(
            f"Device type-day combinations after filtering: {valid_device_type_days}"
        )

        # Show some statistics
        logger.info("Event count distribution:")
        daily_event_counts.groupBy().agg(
            mean("distinct_events").alias("avg_events"),
            percentile_approx("distinct_events", 0.5).alias("median_events"),
            percentile_approx("distinct_events", 0.95).alias("p95_events"),
        ).show()

        # Join back to filter the main dataset
        df_filtered = df.join(
            valid_device_types, on=["device_type", "event_date"], how="inner"
        )

        logger.info(f"Data after device type filtering: {df_filtered.count()} records")

        return df_filtered

    def compute_95th_percentile(self, df: DataFrame) -> DataFrame:
        """
        Compute the 95th percentile of event_duration per device type per day.

        Args:
            df: Filtered DataFrame

        Returns:
            DataFrame with 95th percentile results
        """
        logger.info(
            "Computing 95th percentile of event_duration per device type per day..."
        )

        # Compute percentiles and additional statistics
        percentile_results = df.groupBy("device_type", "event_date").agg(
            percentile_approx("event_duration", 0.95).alias("percentile_95th"),
            percentile_approx("event_duration", 0.5).alias("percentile_50th"),
            percentile_approx("event_duration", 0.75).alias("percentile_75th"),
            percentile_approx("event_duration", 0.25).alias("percentile_25th"),
            mean("event_duration").alias("mean_event_duration"),
            stddev("event_duration").alias("stddev_event_duration"),
            count("event_duration").alias("total_events"),
            countDistinct("device_id").alias("unique_devices"),
        )

        # Round values for better readability
        percentile_results = percentile_results.select(
            col("device_type"),
            col("event_date"),
            spark_round(col("percentile_95th"), 3).alias("percentile_95th"),
            spark_round(col("percentile_50th"), 3).alias("percentile_50th"),
            spark_round(col("percentile_75th"), 3).alias("percentile_75th"),
            spark_round(col("percentile_25th"), 3).alias("percentile_25th"),
            spark_round(col("mean_event_duration"), 3).alias("mean_event_duration"),
            spark_round(col("stddev_event_duration"), 3).alias("stddev_event_duration"),
            col("total_events"),
            col("unique_devices"),
        )

        # Add metadata
        percentile_results = (
            percentile_results.withColumn("computation_timestamp", current_timestamp())
            .withColumn("outlier_method", lit("3_standard_deviations"))
            .withColumn("min_events_threshold", lit(500))
        )

        # Sort by device type and date
        percentile_results = percentile_results.orderBy("device_type", "event_date")

        logger.info(
            f"Computed percentiles for {percentile_results.count()} device type-day combinations"
        )

        return percentile_results

    def write_results_to_s3(
        self, df: DataFrame, output_format: str = "parquet"
    ) -> None:
        """
        Write results to S3/MinIO.

        Args:
            df: Results DataFrame
            output_format: Output format (parquet, csv, json)
        """
        logger.info(
            f"Writing results to S3 in {output_format} format: {self.output_path}"
        )

        try:
            # Write in the specified format
            if output_format.lower() == "parquet":
                (
                    df.coalesce(1)  # Single file for easier handling
                    .write.mode("overwrite")
                    .option("compression", "snappy")
                    .parquet(f"{self.output_path}/percentile_results.parquet")
                )

            elif output_format.lower() == "csv":
                (
                    df.coalesce(1)
                    .write.mode("overwrite")
                    .option("header", "true")
                    .csv(f"{self.output_path}/percentile_results.csv")
                )

            elif output_format.lower() == "json":
                (
                    df.coalesce(1)
                    .write.mode("overwrite")
                    .json(f"{self.output_path}/percentile_results.json")
                )
            else:
                raise ValueError(f"Unsupported output format: {output_format}")

            logger.info("Successfully wrote results to S3")

        except Exception as e:
            logger.error(f"Error writing results to S3: {e}")
            raise

    def generate_validation_report(self, df: DataFrame) -> DataFrame:
        """
        Generate a validation report with summary statistics.

        Args:
            df: Results DataFrame

        Returns:
            Validation report DataFrame
        """
        logger.info("Generating validation report...")

        # Summary statistics
        summary_stats = df.agg(
            count("*").alias("total_records"),
            countDistinct("device_type").alias("unique_device_types"),
            countDistinct("event_date").alias("unique_dates"),
            mean("percentile_95th").alias("avg_95th_percentile"),
            percentile_approx("percentile_95th", 0.5).alias("median_95th_percentile"),
            percentile_approx("total_events", 0.5).alias("median_events_per_day"),
            sum("total_events").alias("total_events_processed"),
        ).collect()[0]

        # Create validation report
        validation_data = [
            ("total_records", summary_stats["total_records"]),
            ("unique_device_types", summary_stats["unique_device_types"]),
            ("unique_dates", summary_stats["unique_dates"]),
            ("avg_95th_percentile", round(summary_stats["avg_95th_percentile"], 3)),
            (
                "median_95th_percentile",
                round(summary_stats["median_95th_percentile"], 3),
            ),
            ("median_events_per_day", summary_stats["median_events_per_day"]),
            ("total_events_processed", summary_stats["total_events_processed"]),
            ("computation_date", datetime.now().isoformat()),
            ("outlier_removal_method", "3_standard_deviations"),
            ("min_events_threshold", 500),
        ]

        validation_df = self.spark.createDataFrame(validation_data, ["metric", "value"])

        return validation_df

    def run_etl_job(
        self, date_filter: Optional[str] = None, output_format: str = "parquet"
    ) -> bool:
        """
        Run the complete ETL job.

        Args:
            date_filter: Optional date filter in YYYY-MM-DD format
            output_format: Output format (parquet, csv, json)

        Returns:
            True if successful, False otherwise
        """
        logger.info("Starting Spark S3 Percentile ETL job...")

        try:
            # Step 1: Read data from S3
            raw_df = self.read_s3_data(date_filter)

            # Step 2: Prepare and clean data
            prepared_df = self.prepare_data(raw_df)

            # Step 3: Remove outliers
            clean_df = self.detect_and_remove_outliers(prepared_df)

            # Step 4: Filter device types by event count
            filtered_df = self.filter_device_types_by_event_count(clean_df)

            # Step 5: Compute 95th percentile
            results_df = self.compute_95th_percentile(filtered_df)

            # Step 6: Show sample results
            logger.info("Sample results:")
            results_df.show(20, truncate=False)

            # Step 7: Write results to S3
            self.write_results_to_s3(results_df, output_format)

            # Step 8: Generate and write validation report
            validation_df = self.generate_validation_report(results_df)
            logger.info("Validation report:")
            validation_df.show(truncate=False)

            # Write validation report
            (
                validation_df.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(f"{self.output_path}/validation_report.csv")
            )

            logger.info("ETL job completed successfully!")
            return True

        except Exception as e:
            logger.error(f"ETL job failed: {e}")
            return False
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()


def main():
    """Main function to run the ETL job."""
    import argparse

    parser = argparse.ArgumentParser(description="Spark S3 Percentile ETL Job")
    parser.add_argument(
        "--input-path", required=True, help="S3 input path (e.g., s3a://bucket/raw/)"
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="S3 output path (e.g., s3a://bucket/results/)",
    )
    parser.add_argument("--date-filter", help="Date filter in YYYY-MM-DD format")
    parser.add_argument(
        "--output-format",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Output format",
    )

    args = parser.parse_args()

    logger.info("Starting Spark S3 Percentile ETL application")
    logger.info(f"Input path: {args.input_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Date filter: {args.date_filter}")
    logger.info(f"Output format: {args.output_format}")

    # Create and run ETL job
    etl = SparkS3PercentileETL(args.input_path, args.output_path)
    success = etl.run_etl_job(args.date_filter, args.output_format)

    if success:
        logger.info("ETL job completed successfully")
        exit(0)
    else:
        logger.error("ETL job failed")
        exit(1)


if __name__ == "__main__":
    main()
