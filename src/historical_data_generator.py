"""
Historical data generator for populating MinIO with a week of IoT data.
"""
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

import boto3
from dotenv import load_dotenv

sys.path.append("producer")

from src.producer.producer import IoTDataGenerator
from src.producer.schema import IoTEvent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HistoricalDataGenerator:
    """Generate historical IoT data for the past week."""

    def __init__(self):
        """Initialize the historical data generator."""
        self.data_generator = IoTDataGenerator()

        # S3/MinIO configuration - no defaults, raise errors if missing
        self.endpoint_url = os.getenv("MINIO_ENDPOINT")
        if not self.endpoint_url:
            raise ValueError("MINIO_ENDPOINT environment variable is required")

        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        if not self.access_key:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")

        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not self.secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")

        self.bucket_name = os.getenv("AWS_S3_BUCKET_DATA")
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_DATA environment variable is required")

        # Create S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="eu-west-1",
            use_ssl=False,
            verify=False,
        )

        logger.info("Initialized Historical Data Generator")
        logger.info(f"Target bucket: {self.bucket_name}")

    def generate_historical_events(
        self, start_date: datetime, end_date: datetime, events_per_hour: int = 120
    ) -> List[IoTEvent]:
        """Generate historical events for a date range."""
        events = []
        current_time = start_date

        logger.info(f"Generating events from {start_date} to {end_date}")
        logger.info(f"Target events per hour: {events_per_hour}")

        while current_time < end_date:
            # Generate events for this hour
            for _ in range(events_per_hour):
                # Create event with historical timestamp
                event = self.data_generator.generate_event()

                # Override timestamp with historical time
                event.timestamp = current_time
                events.append(event)

                # Increment time by a small random amount within the hour
                current_time += timedelta(seconds=3600 / events_per_hour)

            # Move to next hour boundary
            current_time = current_time.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)

        logger.info(f"Generated {len(events)} historical events")
        return events

    def group_events_by_hour(self, events: List[IoTEvent]) -> Dict[str, Any]:
        """Group events by hour for efficient storage."""
        grouped = {}

        for event in events:
            # Create hour key
            hour_key = event.timestamp.strftime("%Y/%m/%d/%H")

            if hour_key not in grouped:
                grouped[hour_key] = []

            grouped[hour_key].append(event)

        logger.info(f"Grouped events into {len(grouped)} hourly batches")
        return grouped

    def upload_hourly_batch(self, hour_key: str, events: List[IoTEvent]) -> bool:
        """Upload an hourly batch of events to S3."""
        try:
            # Convert events to JSON
            events_data = []
            for event in events:
                event_dict = event.model_dump()
                # Ensure timestamp is ISO format string
                event_dict["timestamp"] = event.timestamp.isoformat()
                events_data.append(event_dict)

            # Create S3 key
            s3_key = f"historical/raw/{hour_key}/events.json"

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(events_data, indent=2),
                ContentType="application/json",
            )

            logger.info(f"Uploaded {len(events)} events to {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload batch {hour_key}: {e}")
            return False

    def create_aggregated_data(self, events: List[IoTEvent]) -> List[Any]:
        """Create aggregated data similar to Spark streaming output."""
        # Group by device and hour
        aggregations = {}

        for event in events:
            hour_key = event.timestamp.strftime("%Y-%m-%d %H:00:00")
            device_key = f"{event.device_id}_{hour_key}"

            if device_key not in aggregations:
                aggregations[device_key] = {
                    "window_start": hour_key,
                    "window_end": (
                        event.timestamp.replace(minute=59, second=59)
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "device_id": event.device_id,
                    "device_type": event.device_type,
                    "location": event.location,
                    "unit": event.unit,
                    "values": [],
                    "event_durations": [],
                    "battery_levels": [],
                    "signal_strengths": [],
                }

            # Collect values for aggregation
            agg = aggregations[device_key]
            agg["values"].append(event.value)
            agg["event_durations"].append(event.event_duration)
            if event.battery_level is not None:
                agg["battery_levels"].append(event.battery_level)
            if event.signal_strength is not None:
                agg["signal_strengths"].append(event.signal_strength)

        # Calculate aggregations
        final_aggregations = []
        for device_key, agg in aggregations.items():
            values = agg["values"]
            final_agg = {
                "window_start": agg["window_start"],
                "window_end": agg["window_end"],
                "device_id": agg["device_id"],
                "device_type": agg["device_type"],
                "location": agg["location"],
                "unit": agg["unit"],
                "avg_value": sum(values) / len(values),
                "event_count": len(values),
                "min_value": min(values),
                "max_value": max(values),
                "avg_event_duration": sum(agg["event_durations"])
                / len(agg["event_durations"]),
                "avg_battery_level": sum(agg["battery_levels"])
                / len(agg["battery_levels"])
                if agg["battery_levels"]
                else None,
                "avg_signal_strength": sum(agg["signal_strengths"])
                / len(agg["signal_strengths"])
                if agg["signal_strengths"]
                else None,
                "year": int(agg["window_start"][:4]),
                "month": int(agg["window_start"][5:7]),
                "day": int(agg["window_start"][8:10]),
                "hour": int(agg["window_start"][11:13]),
            }
            final_aggregations.append(final_agg)

        return final_aggregations

    def upload_aggregated_batch(self, hour_key: str, aggregations: List[dict]) -> bool:
        """Upload aggregated data batch to S3."""
        try:
            # Create S3 key
            s3_key = f"historical/aggregated/{hour_key}/aggregations.json"

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(aggregations, indent=2),
                ContentType="application/json",
            )

            logger.info(f"Uploaded {len(aggregations)} aggregations to {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload aggregated batch {hour_key}: {e}")
            return False

    def generate_week_of_data(self, days_back: int = 7) -> bool:
        """Generate a week of historical data."""
        logger.info(f"Starting generation of {days_back} days of historical data")

        # Calculate date range
        end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days_back)

        logger.info(f"Date range: {start_date} to {end_date}")

        # Generate events
        events = self.generate_historical_events(start_date, end_date)

        # Group by hour
        grouped_events = self.group_events_by_hour(events)

        # Upload each hourly batch
        success_count = 0
        total_batches = len(grouped_events)

        for hour_key, hourly_events in grouped_events.items():
            # Upload raw data
            if self.upload_hourly_batch(hour_key, hourly_events):
                success_count += 1

            # Create and upload aggregated data
            aggregations = self.create_aggregated_data(hourly_events)
            self.upload_aggregated_batch(hour_key, aggregations)

            if success_count % 24 == 0:  # Log progress every day
                logger.info(f"Processed {success_count}/{total_batches} hourly batches")

        logger.info(
            f"Historical data generation completed: {success_count}/{total_batches} batches uploaded"
        )
        return success_count == total_batches


def main():
    """Main function to generate historical data."""
    logger.info("Starting historical data generation...")

    # Get days back from environment - default to 7 if not specified
    days_back_str = os.getenv("HISTORICAL_DAYS", "7")
    try:
        days_back = int(days_back_str)
    except ValueError:
        raise ValueError("HISTORICAL_DAYS must be a valid integer")

    generator = HistoricalDataGenerator()
    success = generator.generate_week_of_data(days_back)

    if success:
        logger.info("Historical data generation completed successfully")
        exit(0)
    else:
        logger.error("Historical data generation failed")
        exit(1)


if __name__ == "__main__":
    main()
