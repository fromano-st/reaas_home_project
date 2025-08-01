"""
Kafka consumer with aggregations and Parquet output to MinIO.
Similar to streaming_etl.py but using pure Python instead of Spark.
"""
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from threading import Lock
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError, ProcessingError
from pydantic import BaseModel, Field, ValidationError
from prometheus_client import start_http_server, Counter, Gauge
import boto3

# Load environment variables
load_dotenv()

# Initialize metrics
MESSAGES_PROCESSED = Counter("etl_messages_processed", "Total messages processed")
PROCESSING_FAILURES = Counter(
    "etl_processing_failures", "Total processing failures", ["reason"]
)
CONSUMER_LAG = Gauge("etl_consumer_lag", "Current consumer lag in messages")
PROCESSING_TIME = Gauge("etl_processing_time_ms", "Message processing time in ms")

# Start metrics server (in your main application)
start_http_server(8000)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IoTEventValidator(BaseModel):
    """Validator for IoT event data."""

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


@dataclass
class ProcessedEvent:
    """Processed IoT event with additional quality flags."""

    event_duration: float
    device_type: str
    device_id: str
    timestamp: datetime
    location: str
    value: float
    unit: str
    battery_level: Optional[int]
    signal_strength: Optional[int]
    metadata: Dict[str, Any]
    is_valid_battery: bool
    is_valid_signal: bool
    processing_timestamp: datetime


@dataclass
class WindowedAggregation:
    """Windowed aggregation result."""

    window_start: datetime
    window_end: datetime
    device_id: str
    device_type: str
    location: str
    unit: str
    avg_value: float
    event_count: int
    min_value: float
    max_value: float
    stddev_value: float
    avg_event_duration: float
    avg_battery_level: Optional[float]
    avg_signal_strength: Optional[float]
    low_battery_count: int
    weak_signal_count: int
    year: int
    month: int
    day: int
    hour: int


class TimeWindowManager:
    """Manages time-based windowing for aggregations."""

    def __init__(self, window_size_minutes: int = 1, watermark_minutes: int = 2):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.watermark = timedelta(minutes=watermark_minutes)
        self.windows: Dict[Tuple[datetime, str], List[ProcessedEvent]] = defaultdict(
            list
        )
        self.completed_windows: deque = deque(
            maxlen=1000
        )  # Keep last 1000 completed windows
        self.lock = Lock()

    def get_window_start(self, timestamp: datetime) -> datetime:
        """Get the window start time for a given timestamp."""
        # Round down to the nearest minute
        return timestamp.replace(second=0, microsecond=0)

    def add_event(self, event: ProcessedEvent) -> List[WindowedAggregation]:
        """Add an event to the appropriate window and return completed windows."""
        with self.lock:
            window_start = self.get_window_start(event.timestamp)
            window_key = (window_start, event.device_id)

            self.windows[window_key].append(event)

            # Check for completed windows (beyond watermark)
            current_time = datetime.now(timezone.utc)
            watermark_time = current_time - self.watermark

            completed = []
            keys_to_remove = []

            for (win_start, device_id), events in self.windows.items():
                if win_start < watermark_time:
                    # Window is complete, create aggregation
                    agg = self._create_aggregation(win_start, device_id, events)
                    completed.append(agg)
                    keys_to_remove.append((win_start, device_id))

            # Remove completed windows
            for key in keys_to_remove:
                del self.windows[key]

            return completed

    def _create_aggregation(
        self, window_start: datetime, device_id: str, events: List[ProcessedEvent]
    ) -> WindowedAggregation:
        """Create aggregation from events in a window."""
        if not events:
            raise ValueError("Cannot create aggregation from empty events list")

        # Get common attributes from first event
        first_event = events[0]
        window_end = window_start + self.window_size

        # Calculate aggregations
        values = [e.value for e in events]
        durations = [e.event_duration for e in events]
        battery_levels = [
            e.battery_level for e in events if e.battery_level is not None
        ]
        signal_strengths = [
            e.signal_strength for e in events if e.signal_strength is not None
        ]

        # Calculate statistics
        avg_value = sum(values) / len(values)
        min_value = min(values)
        max_value = max(values)

        # Standard deviation calculation
        if len(values) > 1:
            mean = avg_value
            variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
            stddev_value = variance**0.5
        else:
            stddev_value = 0.0

        # Battery and signal aggregations
        avg_battery = (
            sum(battery_levels) / len(battery_levels) if battery_levels else None
        )
        avg_signal = (
            sum(signal_strengths) / len(signal_strengths) if signal_strengths else None
        )

        # Quality counts
        low_battery_count = sum(1 for e in events if not e.is_valid_battery)
        weak_signal_count = sum(1 for e in events if not e.is_valid_signal)

        return WindowedAggregation(
            window_start=window_start,
            window_end=window_end,
            device_id=device_id,
            device_type=first_event.device_type,
            location=first_event.location,
            unit=first_event.unit,
            avg_value=avg_value,
            event_count=len(events),
            min_value=min_value,
            max_value=max_value,
            stddev_value=stddev_value,
            avg_event_duration=sum(durations) / len(durations),
            avg_battery_level=avg_battery,
            avg_signal_strength=avg_signal,
            low_battery_count=low_battery_count,
            weak_signal_count=weak_signal_count,
            year=window_start.year,
            month=window_start.month,
            day=window_start.day,
            hour=window_start.hour,
        )


class ParquetS3Writer:
    """Handles writing Parquet files to MinIO/S3."""

    def __init__(self):
        # S3 Configuration
        self.endpoint_url = os.getenv("AWS_ENDPOINT")
        if not self.endpoint_url:
            raise ValueError("AWS_ENDPOINT environment variable is required")

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

        logger.info(f"Initialized S3 writer for bucket: {self.bucket_name}")

    def write_raw_events(self, events: List[ProcessedEvent]) -> bool:
        """Write raw events to S3 in Parquet format."""
        if not events:
            return True

        try:
            # Convert to DataFrame
            data = []
            for event in events:
                event_dict = asdict(event)
                # Convert datetime objects to strings for Parquet compatibility
                event_dict["timestamp"] = event.timestamp.isoformat()
                event_dict[
                    "processing_timestamp"
                ] = event.processing_timestamp.isoformat()
                data.append(event_dict)

            df = pd.DataFrame(data)

            # Add partitioning columns
            df["year"] = df["timestamp"].apply(lambda x: datetime.fromisoformat(x).year)
            df["month"] = df["timestamp"].apply(
                lambda x: datetime.fromisoformat(x).month
            )
            df["day"] = df["timestamp"].apply(lambda x: datetime.fromisoformat(x).day)
            df["hour"] = df["timestamp"].apply(lambda x: datetime.fromisoformat(x).hour)

            # Create Parquet file in memory
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)

            # Generate S3 key with partitioning
            first_event = events[0]
            timestamp = first_event.timestamp
            s3_key = f"raw/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={timestamp.hour:02d}/events_{int(time.time())}.parquet"

            # Upload to S3
            buffer.seek(0)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )

            logger.info(
                f"✓ Wrote {len(events)} raw events to s3://{self.bucket_name}/{s3_key}"
            )
            return True

        except Exception as e:
            logger.error(f"✗ Failed to write raw events: {e}")
            return False

    def write_aggregations(self, aggregations: List[WindowedAggregation]) -> bool:
        """Write aggregated data to S3 in Parquet format."""
        if not aggregations:
            return True

        try:
            # Convert to DataFrame
            data = []
            for agg in aggregations:
                agg_dict = asdict(agg)
                # Convert datetime objects to strings for Parquet compatibility
                agg_dict["window_start"] = agg.window_start.isoformat()
                agg_dict["window_end"] = agg.window_end.isoformat()
                data.append(agg_dict)

            df = pd.DataFrame(data)

            # Create Parquet file in memory
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)

            # Generate S3 key with partitioning
            first_agg = aggregations[0]
            s3_key = f"aggregated/year={first_agg.year}/month={first_agg.month:02d}/day={first_agg.day:02d}/hour={first_agg.hour:02d}/agg_{int(time.time())}.parquet"

            # Upload to S3
            buffer.seek(0)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )

            logger.info(
                f"✓ Wrote {len(aggregations)} aggregations to s3://{self.bucket_name}/{s3_key}"
            )
            return True

        except Exception as e:
            logger.error(f"✗ Failed to write aggregations: {e}")
            return False


class KafkaAggregator:
    """Main Kafka consumer with aggregation and Parquet output."""

    def __init__(self):
        """Initialize the Kafka aggregator."""
        # Kafka Configuration
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

        self.topic = os.getenv("KAFKA_TOPIC")
        if not self.topic:
            raise ValueError("KAFKA_TOPIC environment variable is required")

        # Processing configuration
        self.batch_size = int(os.getenv("AGGREGATOR_BATCH_SIZE", "100"))
        self.flush_interval = int(
            os.getenv("AGGREGATOR_FLUSH_INTERVAL", "30")
        )  # seconds

        # Initialize components
        self.window_manager = TimeWindowManager()
        self.parquet_writer = ParquetS3Writer()
        self.consumer = None

        # Processing state
        self.running = True
        self.processed_events = []
        self.events_processed = 0
        self.events_written = 0
        self.aggregations_written = 0
        self.last_flush_time = time.time()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("Initialized Kafka Aggregator")
        logger.info(f"Kafka servers: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Flush interval: {self.flush_interval}s")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def create_consumer(self) -> bool:
        """Create and configure the Kafka consumer."""
        try:
            logger.info("Creating Kafka consumer...")

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="kafka-aggregator",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                consumer_timeout_ms=1000,  # 1 second timeout for responsive shutdown
                max_poll_records=self.batch_size,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

            logger.info("Kafka consumer created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            return False

    def validate_and_process_message(
        self, message_value: Dict[str, Any]
    ) -> Optional[ProcessedEvent]:
        """Validate and process a single message."""
        try:
            # Validate using Pydantic model
            validated_event = IoTEventValidator(**message_value)

            # Parse timestamp
            timestamp = datetime.fromisoformat(
                validated_event.timestamp.replace("Z", "+00:00")
            )
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Apply data quality checks (similar to streaming_etl.py)
            if not self._is_valid_event(validated_event):
                return None

            # Create processed event with quality flags
            processed_event = ProcessedEvent(
                event_duration=validated_event.event_duration,
                device_type=validated_event.device_type,
                device_id=validated_event.device_id,
                timestamp=timestamp,
                location=validated_event.location,
                value=validated_event.value,
                unit=validated_event.unit,
                battery_level=validated_event.battery_level,
                signal_strength=validated_event.signal_strength,
                metadata=validated_event.metadata or {},
                is_valid_battery=self._is_valid_battery(validated_event.battery_level),
                is_valid_signal=self._is_valid_signal(validated_event.signal_strength),
                processing_timestamp=datetime.now(timezone.utc),
            )

            return processed_event

        except ValidationError as e:
            logger.debug(f"Invalid message schema: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error processing message: {e}")
            return None

    def _is_valid_event(self, event: IoTEventValidator) -> bool:
        """Apply data quality checks similar to streaming_etl.py."""
        # Check event duration
        if event.event_duration <= 0 or event.event_duration > 3600:
            return False

        # Check value is not NaN
        if pd.isna(event.value):
            return False

        # Check battery level range
        if event.battery_level is not None:
            if event.battery_level < 0 or event.battery_level > 100:
                return False

        # Check signal strength range
        if event.signal_strength is not None:
            if event.signal_strength < -120 or event.signal_strength > -30:
                return False

        return True

    def _is_valid_battery(self, battery_level: Optional[int]) -> bool:
        """Check if battery level is valid (>= 20%)."""
        if battery_level is None:
            return True
        return battery_level >= 20

    def _is_valid_signal(self, signal_strength: Optional[int]) -> bool:
        """Check if signal strength is valid (>= -90 dBm)."""
        if signal_strength is None:
            return True
        return signal_strength >= -90

    def flush_data(self) -> None:
        """Flush accumulated data to storage."""
        if not self.processed_events:
            return

        logger.info(f"Flushing {len(self.processed_events)} events to storage...")

        # Write raw events
        if self.parquet_writer.write_raw_events(self.processed_events):
            self.events_written += len(self.processed_events)

        # Clear processed events
        self.processed_events.clear()
        self.last_flush_time = time.time()

    def process_aggregations(self, aggregations: List[WindowedAggregation]) -> None:
        """Process and write completed aggregations."""
        if not aggregations:
            return

        logger.info(f"Processing {len(aggregations)} completed window aggregations...")

        # Write aggregations
        if self.parquet_writer.write_aggregations(aggregations):
            self.aggregations_written += len(aggregations)

    def print_statistics(self) -> None:
        """Print processing statistics."""
        logger.info("\n" + "=" * 60)
        logger.info("KAFKA AGGREGATOR STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Events processed: {self.events_processed}")
        logger.info(f"Raw events written: {self.events_written}")
        logger.info(f"Aggregations written: {self.aggregations_written}")
        logger.info(f"Active windows: {len(self.window_manager.windows)}")
        logger.info("=" * 60)

    def run(self) -> bool:
        """Run the main aggregation loop."""
        if not self.create_consumer():
            return False

        logger.info("Starting Kafka aggregation processing...")
        logger.info("Press Ctrl+C to stop gracefully")

        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    if not message_batch:
                        # Check if we need to flush based on time
                        if time.time() - self.last_flush_time >= self.flush_interval:
                            self.flush_data()
                        continue

                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                if not self.running:
                                    break
                                start_time = time.time()

                                # Process message
                                processed_event = self.validate_and_process_message(
                                    message.value
                                )
                                MESSAGES_PROCESSED.inc()
                                PROCESSING_TIME.set(time.time() - start_time)
                                # Get and record consumer lag
                                position = self.consumer.position(
                                    self.consumer.assignment()
                                )
                                end_offsets = self.consumer.end_offsets(
                                    self.consumer.assignment()
                                )
                                lag = sum(
                                    end - pos
                                    for (tp, end), (_, pos) in zip(
                                        end_offsets.items(), position.items()
                                    )
                                )
                                CONSUMER_LAG.set(lag)
                                if processed_event:
                                    self.processed_events.append(processed_event)
                                    self.events_processed += 1

                                    # Add to window manager and get completed aggregations
                                    completed_aggregations = (
                                        self.window_manager.add_event(processed_event)
                                    )
                                    if completed_aggregations:
                                        self.process_aggregations(
                                            completed_aggregations
                                        )

                                    # Log progress
                                    if self.events_processed % 100 == 0:
                                        logger.info(
                                            f"Processed {self.events_processed} events"
                                        )
                            except ProcessingError as e:
                                PROCESSING_FAILURES.labels(reason=str(e)).inc()
                            except Exception:
                                PROCESSING_FAILURES.labels(
                                    reason="unexpected_error"
                                ).inc()

                    # Check if we need to flush
                    if (
                        len(self.processed_events) >= self.batch_size
                        or time.time() - self.last_flush_time >= self.flush_interval
                    ):
                        self.flush_data()

                except KafkaTimeoutError:
                    # Normal timeout, continue
                    continue
                except Exception as e:
                    logger.error(f"Error processing messages: {e}")
                    continue

            # Final flush
            logger.info("Performing final data flush...")
            self.flush_data()

            # Print final statistics
            self.print_statistics()

            return True

        except Exception as e:
            logger.error(f"Fatal error in aggregation loop: {e}")
            return False
        finally:
            if self.consumer:
                logger.info("Closing Kafka consumer...")
                self.consumer.close()


def main():
    """Main function to run the Kafka aggregator."""
    logger.info("Starting Kafka Aggregator with Parquet output...")

    try:
        aggregator = KafkaAggregator()
        success = aggregator.run()

        if success:
            logger.info("Kafka aggregator completed successfully")
            sys.exit(0)
        else:
            logger.error("Kafka aggregator failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Aggregator interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
