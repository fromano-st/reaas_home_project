"""
Validation script for Spark Streaming Tester setup.
Validates all components are properly configured and ready.
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


def validate_files():
    """Validate all required files exist."""
    logger.info("=== File Structure Validation ===")

    required_files = [
        "test_spark_streaming.py",
        "requirements.txt",
        "Dockerfile",
        "README.md",
        "test_config.py",
    ]

    success = True
    for file_name in required_files:
        if os.path.exists(file_name):
            logger.info(f"✓ {file_name}: EXISTS")
        else:
            logger.error(f"❌ {file_name}: MISSING")
            success = False

    return success


def validate_docker_compose():
    """Validate docker-compose configuration."""
    logger.info("=== Docker Compose Validation ===")

    compose_file = "../../docker-compose.yml"
    if os.path.exists(compose_file):
        logger.info("✓ docker-compose.yml: EXISTS")

        # Check if our service is defined
        with open(compose_file, "r") as f:
            content = f.read()
            if "spark-streaming-tester:" in content:
                logger.info("✓ spark-streaming-tester service: DEFINED")

                # Check dependencies
                if (
                    "depends_on:" in content
                    and "kafka" in content
                    and "iot-producer" in content
                ):
                    logger.info("✓ Service dependencies: CONFIGURED")
                else:
                    logger.warning("⚠ Service dependencies: CHECK CONFIGURATION")

                return True
            else:
                logger.error("❌ spark-streaming-tester service: NOT DEFINED")
                return False
    else:
        logger.error("❌ docker-compose.yml: NOT FOUND")
        return False


def validate_environment():
    """Validate environment configuration."""
    logger.info("=== Environment Validation ===")

    # Check .env file
    env_file = "../../.env"
    if os.path.exists(env_file):
        logger.info("✓ .env file: EXISTS")
    else:
        logger.warning("⚠ .env file: NOT FOUND (using system environment)")

    # Required variables
    required_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC"),
    }

    success = True
    for var_name, var_value in required_vars.items():
        if var_value:
            logger.info(f"✓ {var_name}: {var_value}")
        else:
            logger.error(f"❌ {var_name}: NOT SET")
            success = False

    # Optional variables
    optional_vars = {
        "CONSUMER_TIMEOUT": os.getenv("CONSUMER_TIMEOUT", "60"),
        "PRODUCER_TIMEOUT": os.getenv("PRODUCER_TIMEOUT", "30"),
        "MAX_MESSAGES": os.getenv("MAX_MESSAGES", "10"),
        "TEST_DURATION": os.getenv("TEST_DURATION", "30"),
    }

    for var_name, var_value in optional_vars.items():
        logger.info(f"✓ {var_name}: {var_value} (default applied if not set)")

    return success


def validate_requirements():
    """Validate requirements.txt content."""
    logger.info("=== Requirements Validation ===")

    if os.path.exists("requirements.txt"):
        with open("requirements.txt", "r") as f:
            requirements = f.read().strip().split("\n")

        expected_packages = ["pyspark", "kafka-python", "python-dotenv"]

        success = True
        for package in expected_packages:
            found = any(package in req for req in requirements)
            if found:
                logger.info(f"✓ {package}: INCLUDED")
            else:
                logger.error(f"❌ {package}: MISSING")
                success = False

        return success
    else:
        logger.error("❌ requirements.txt: NOT FOUND")
        return False


def validate_dockerfile():
    """Validate Dockerfile content."""
    logger.info("=== Dockerfile Validation ===")

    if os.path.exists("Dockerfile"):
        with open("Dockerfile", "r") as f:
            dockerfile_content = f.read()

        checks = {
            "Java Runtime": "openjdk" in dockerfile_content,
            "Python Installation": "python3" in dockerfile_content,
            "Requirements Copy": "requirements.txt" in dockerfile_content,
            "Environment Variables": "PYTHONUNBUFFERED" in dockerfile_content,
            "Default Command": "CMD" in dockerfile_content,
        }

        success = True
        for check_name, check_result in checks.items():
            if check_result:
                logger.info(f"✓ {check_name}: CONFIGURED")
            else:
                logger.error(f"❌ {check_name}: MISSING")
                success = False

        return success
    else:
        logger.error("❌ Dockerfile: NOT FOUND")
        return False


def print_usage_instructions():
    """Print usage instructions."""
    logger.info("=== Usage Instructions ===")
    logger.info("To run the Spark Streaming Tester:")
    logger.info("")
    logger.info("1. Using Docker Compose (recommended):")
    logger.info("   cd /workspace")
    logger.info("   docker-compose up spark-streaming-tester")
    logger.info("")
    logger.info("2. View logs:")
    logger.info("   docker-compose logs -f spark-streaming-tester")
    logger.info("")
    logger.info("3. Run configuration test only:")
    logger.info("   cd tests/spark_streaming_tester")
    logger.info("   python3 test_config.py")
    logger.info("")
    logger.info("4. Environment variables can be customized in .env file:")
    logger.info("   CONSUMER_TIMEOUT=60")
    logger.info("   PRODUCER_TIMEOUT=30")
    logger.info("   MAX_MESSAGES=10")
    logger.info("   TEST_DURATION=30")


def main():
    """Main validation function."""
    logger.info("Starting Spark Streaming Tester Setup Validation...")
    logger.info("=" * 60)

    all_checks_passed = True

    # Run all validations
    validations = [
        ("File Structure", validate_files),
        ("Docker Compose", validate_docker_compose),
        ("Environment", validate_environment),
        ("Requirements", validate_requirements),
        ("Dockerfile", validate_dockerfile),
    ]

    for validation_name, validation_func in validations:
        try:
            if not validation_func():
                all_checks_passed = False
        except Exception as e:
            logger.error(f"❌ {validation_name} validation failed: {e}")
            all_checks_passed = False

        logger.info("")  # Add spacing between validations

    # Print results
    logger.info("=" * 60)
    if all_checks_passed:
        logger.info("🎉 ALL VALIDATIONS PASSED!")
        logger.info("The Spark Streaming Tester is ready to use.")
        print_usage_instructions()
        sys.exit(0)
    else:
        logger.error("❌ SOME VALIDATIONS FAILED!")
        logger.error("Please fix the issues above before using the tester.")
        sys.exit(1)


if __name__ == "__main__":
    main()
