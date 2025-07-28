"""
Configuration test for Spark Streaming Tester.
Tests environment variables and basic functionality without requiring PySpark.
"""
import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_configuration():
    """Test configuration and environment variables."""
    logger.info("Testing Spark Streaming Tester configuration...")

    success = True

    # Required environment variables
    required_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC"),
    }

    # Optional environment variables with defaults
    optional_vars = {
        "CONSUMER_TIMEOUT": os.getenv("CONSUMER_TIMEOUT", "60"),
        "PRODUCER_TIMEOUT": os.getenv("PRODUCER_TIMEOUT", "30"),
        "MAX_MESSAGES": os.getenv("MAX_MESSAGES", "10"),
        "TEST_DURATION": os.getenv("TEST_DURATION", "30"),
    }

    # Check required variables
    logger.info("=== Required Environment Variables ===")
    for var_name, var_value in required_vars.items():
        if var_value:
            logger.info(f"✓ {var_name}: {var_value}")
        else:
            logger.error(f"❌ {var_name}: NOT SET")
            success = False

    # Check optional variables
    logger.info("=== Optional Environment Variables (with defaults) ===")
    for var_name, var_value in optional_vars.items():
        logger.info(f"✓ {var_name}: {var_value}")

    # Test configuration parsing
    logger.info("=== Configuration Parsing ===")
    try:
        consumer_timeout = int(optional_vars["CONSUMER_TIMEOUT"])
        producer_timeout = int(optional_vars["PRODUCER_TIMEOUT"])
        max_messages = int(optional_vars["MAX_MESSAGES"])
        test_duration = int(optional_vars["TEST_DURATION"])

        logger.info(f"✓ Consumer timeout: {consumer_timeout}s")
        logger.info(f"✓ Producer timeout: {producer_timeout}s")
        logger.info(f"✓ Max messages: {max_messages}")
        logger.info(f"✓ Test duration: {test_duration}s")

    except ValueError as e:
        logger.error(f"❌ Configuration parsing error: {e}")
        success = False

    # Test schema definition (mock)
    logger.info("=== Schema Definition Test ===")
    try:
        schema_fields = [
            "event_duration",
            "device_type",
            "device_id",
            "timestamp",
            "location",
            "value",
            "unit",
            "battery_level",
            "signal_strength",
            "metadata",
        ]
        logger.info(f"✓ Schema fields defined: {', '.join(schema_fields)}")
    except Exception as e:
        logger.error(f"❌ Schema definition error: {e}")
        success = False

    return success


def main():
    """Main function to run configuration tests."""
    logger.info("Starting Spark Streaming Tester Configuration Test...")

    try:
        success = test_configuration()

        if success:
            logger.info("✅ Configuration test completed successfully")
            logger.info("The Spark Streaming Tester is properly configured!")
            sys.exit(0)
        else:
            logger.error("❌ Configuration test failed")
            logger.error("Please check your environment variables and configuration")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
