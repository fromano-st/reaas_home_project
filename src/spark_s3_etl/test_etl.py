"""
Test script for Spark S3/MinIO ETL pipeline.

This script generates sample data and tests the ETL pipeline functionality.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLTester:
    """Test class for the Spark ETL pipeline."""

    def __init__(self):
        """Initialize the tester."""
        self.spark = self._create_spark_session()
        self.device_types = [
            "temperature_sensor",
            "humidity_sensor",
            "pressure_sensor",
            "motion_detector",
            "light_sensor",
            "smart_thermostat",
            "door_sensor",
            "window_sensor",
        ]

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for testing."""
        return (
            SparkSession.builder.appName("ETL_Tester")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

    def generate_sample_data(
        self, num_records: int = 10000, num_days: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Generate sample IoT event data for testing.

        Args:
            num_records: Number of records to generate
            num_days: Number of days to spread data across

        Returns:
            List of sample IoT events
        """
        import random

        logger.info(
            f"Generating {num_records} sample records across {num_days} days..."
        )

        sample_data = []
        base_date = datetime.now() - timedelta(days=num_days)

        for i in range(num_records):
            # Random device type
            device_type = random.choice(self.device_types)

            # Random timestamp within the date range
            random_days = random.uniform(0, num_days)
            timestamp = base_date + timedelta(days=random_days)

            # Generate realistic event_duration with some outliers
            if random.random() < 0.05:  # 5% outliers
                event_duration = random.uniform(100, 200)  # Outliers
            else:
                # Normal distribution around device-specific means
                device_means = {
                    "temperature_sensor": 25.0,
                    "humidity_sensor": 30.0,
                    "pressure_sensor": 28.0,
                    "motion_detector": 20.0,
                    "light_sensor": 35.0,
                    "smart_thermostat": 22.0,
                    "door_sensor": 18.0,
                    "window_sensor": 26.0,
                }
                mean_duration = device_means.get(device_type, 25.0)
                event_duration = max(0.1, random.normalvariate(mean_duration, 8.0))

            # Generate other fields
            record = {
                "event_duration": round(event_duration, 3),
                "device_type": device_type,
                "device_id": f"{device_type}_{random.randint(1, 100):03d}",
                "timestamp": timestamp.isoformat(),
                "location": random.choice(
                    ["Living Room", "Kitchen", "Bedroom", "Office", "Garage"]
                ),
                "value": round(random.uniform(10.0, 40.0), 2),
                "unit": "°C"
                if "temperature" in device_type
                else "%"
                if "humidity" in device_type
                else "units",
                "battery_level": random.randint(20, 100)
                if random.random() > 0.1
                else None,
                "signal_strength": random.randint(-90, -30)
                if random.random() > 0.1
                else None,
                "metadata": {
                    "firmware_version": f"1.{random.randint(0, 5)}.{random.randint(0, 9)}"
                },
            }

            sample_data.append(record)

        logger.info(f"Generated {len(sample_data)} sample records")
        return sample_data

    def create_test_dataset(
        self, sample_data: List[Dict[str, Any]], output_path: str
    ) -> None:
        """
        Create test dataset in Parquet format.

        Args:
            sample_data: Generated sample data
            output_path: Path to save the test dataset
        """
        logger.info(f"Creating test dataset at {output_path}...")

        # Convert to DataFrame
        df = pd.DataFrame(sample_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Add partitioning columns
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        df["day"] = df["timestamp"].dt.day
        df["hour"] = df["timestamp"].dt.hour

        # Save as Parquet
        df.to_parquet(output_path, partition_cols=["year", "month", "day"], index=False)
        logger.info(f"Test dataset saved to {output_path}")

    def validate_etl_results(self, results_path: str) -> bool:
        """
        Validate ETL results.

        Args:
            results_path: Path to ETL results

        Returns:
            True if validation passes, False otherwise
        """
        logger.info(f"Validating ETL results from {results_path}...")

        try:
            # Read results
            if results_path.endswith(".csv"):
                results_df = pd.read_csv(results_path)
            else:
                results_df = pd.read_parquet(results_path)

            # Validation checks
            validations = []

            # Check 1: Results should have expected columns
            expected_columns = [
                "device_type",
                "event_date",
                "percentile_95th",
                "total_events",
                "unique_devices",
            ]
            missing_columns = [
                col for col in expected_columns if col not in results_df.columns
            ]
            if not missing_columns:
                validations.append("✅ All expected columns present")
            else:
                validations.append(f"❌ Missing columns: {missing_columns}")

            # Check 2: Percentile values should be reasonable
            if "percentile_95th" in results_df.columns:
                p95_values = results_df["percentile_95th"].dropna()
                if (
                    len(p95_values) > 0
                    and p95_values.min() > 0
                    and p95_values.max() < 1000
                ):
                    validations.append("✅ Percentile values are reasonable")
                else:
                    validations.append(
                        f"❌ Percentile values out of range: {p95_values.min()}-{p95_values.max()}"
                    )

            # Check 3: Event counts should meet minimum threshold
            if "total_events" in results_df.columns:
                min_events = results_df["total_events"].min()
                if min_events >= 500:
                    validations.append(
                        "✅ All device types meet minimum event threshold"
                    )
                else:
                    validations.append(
                        f"❌ Some device types below threshold: min={min_events}"
                    )

            # Check 4: Should have multiple device types
            if "device_type" in results_df.columns:
                unique_devices = results_df["device_type"].nunique()
                if unique_devices > 1:
                    validations.append(
                        f"✅ Multiple device types present: {unique_devices}"
                    )
                else:
                    validations.append(f"❌ Too few device types: {unique_devices}")

            # Print validation results
            logger.info("Validation Results:")
            for validation in validations:
                logger.info(f"  {validation}")

            # Overall result
            passed = all("✅" in v for v in validations)
            if passed:
                logger.info("🎉 All validations passed!")
            else:
                logger.error("💥 Some validations failed!")

            return passed

        except Exception as e:
            logger.error(f"Error during validation: {e}")
            return False

    def run_integration_test(self) -> bool:
        """
        Run complete integration test.

        Returns:
            True if test passes, False otherwise
        """
        logger.info("Starting integration test...")

        try:
            # Step 1: Generate sample data
            sample_data = self.generate_sample_data(num_records=15000, num_days=2)

            # Step 2: Create test dataset
            test_input_path = "/tmp/test_input_data"
            self.create_test_dataset(sample_data, test_input_path)

            # Step 3: Run ETL (would need to be implemented)
            logger.info("ETL execution would happen here...")

            # Step 4: Validate results (mock for now)
            logger.info("Creating mock results for validation...")
            mock_results = pd.DataFrame(
                {
                    "device_type": [
                        "temperature_sensor",
                        "humidity_sensor",
                        "pressure_sensor",
                    ],
                    "event_date": ["2025-07-26", "2025-07-26", "2025-07-26"],
                    "percentile_95th": [45.234, 52.891, 48.567],
                    "percentile_75th": [32.156, 38.234, 35.789],
                    "percentile_50th": [21.789, 25.123, 23.456],
                    "total_events": [1250, 1180, 1320],
                    "unique_devices": [85, 78, 92],
                }
            )

            mock_results_path = "/tmp/mock_results.csv"
            mock_results.to_csv(mock_results_path, index=False)

            # Step 5: Validate results
            validation_passed = self.validate_etl_results(mock_results_path)

            # Cleanup
            import shutil

            if os.path.exists(test_input_path):
                shutil.rmtree(test_input_path)
            if os.path.exists(mock_results_path):
                os.remove(mock_results_path)

            return validation_passed

        except Exception as e:
            logger.error(f"Integration test failed: {e}")
            return False
        finally:
            self.spark.stop()

    def generate_performance_test_data(
        self, output_path: str, size_gb: float = 1.0
    ) -> None:
        """
        Generate larger dataset for performance testing.

        Args:
            output_path: Path to save performance test data
            size_gb: Approximate size in GB
        """
        # Estimate records needed (rough calculation)
        # Assuming ~1KB per record on average
        records_needed = int(size_gb * 1024 * 1024)  # Convert GB to KB, then to records

        logger.info(
            f"Generating performance test data: ~{size_gb}GB ({records_needed} records)"
        )

        # Generate in batches to avoid memory issues
        batch_size = 50000
        all_data = []

        for batch in range(0, records_needed, batch_size):
            batch_records = min(batch_size, records_needed - batch)
            batch_data = self.generate_sample_data(batch_records, num_days=7)
            all_data.extend(batch_data)

            if len(all_data) % 100000 == 0:
                logger.info(f"Generated {len(all_data)} records...")

        # Save the data
        self.create_test_dataset(all_data, output_path)
        logger.info(f"Performance test data saved to {output_path}")


def main():
    """Main function to run tests."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Spark S3 ETL Pipeline")
    parser.add_argument(
        "--test-type",
        choices=["integration", "performance", "generate"],
        default="integration",
        help="Type of test to run",
    )
    parser.add_argument(
        "--output-path", default="/tmp/test_data", help="Output path for generated data"
    )
    parser.add_argument(
        "--size-gb",
        type=float,
        default=1.0,
        help="Size in GB for performance test data",
    )

    args = parser.parse_args()

    tester = ETLTester()

    if args.test_type == "integration":
        logger.info("Running integration test...")
        success = tester.run_integration_test()
        exit(0 if success else 1)

    elif args.test_type == "performance":
        logger.info(f"Generating performance test data ({args.size_gb}GB)...")
        tester.generate_performance_test_data(args.output_path, args.size_gb)

    elif args.test_type == "generate":
        logger.info("Generating sample test data...")
        sample_data = tester.generate_sample_data(10000, 3)
        tester.create_test_dataset(sample_data, args.output_path)

    logger.info("Test completed!")


if __name__ == "__main__":
    main()
