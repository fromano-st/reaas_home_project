"""
S3/MinIO connectivity and bucket testing script.
"""
import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class S3Tester:
    """Test S3/MinIO connectivity and operations."""

    def __init__(self):
        """Initialize S3 client."""
        # Configuration from environment - no defaults, raise errors if missing
        self.endpoint_url = os.getenv("AWS_ENDPOINT")
        if not self.endpoint_url:
            raise ValueError("AWS_ENDPOINT environment variable is required")

        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        if not self.access_key:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")

        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not self.secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")

        self.landing_bucket = os.getenv("AWS_S3_BUCKET_LANDING")
        if not self.landing_bucket:
            raise ValueError("AWS_S3_BUCKET_LANDING environment variable is required")

        self.data_bucket = os.getenv("AWS_S3_BUCKET_DATA")
        if not self.data_bucket:
            raise ValueError("AWS_S3_BUCKET_DATA environment variable is required")

        logger.info(f"Initializing S3 client with endpoint: {self.endpoint_url}")

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

    def test_connection(self) -> bool:
        """Test basic S3 connection."""
        try:
            logger.info("Testing S3 connection...")
            response = self.s3_client.list_buckets()
            logger.info(
                f"Connection successful. Found {len(response['Buckets'])} buckets"
            )
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def list_buckets(self) -> list:
        """List all buckets."""
        try:
            logger.info("Listing buckets...")
            response = self.s3_client.list_buckets()
            buckets = [bucket["Name"] for bucket in response["Buckets"]]
            logger.info(f"Buckets: {buckets}")
            return buckets
        except Exception as e:
            logger.error(f"Failed to list buckets: {e}")
            return []

    def create_bucket_if_not_exists(self, bucket_name: str) -> bool:
        """Create bucket if it doesn't exist."""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                # Bucket doesn't exist, create it
                try:
                    logger.info(f"Creating bucket '{bucket_name}'...")
                    self.s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Bucket '{bucket_name}' created successfully")
                    return True
                except Exception as create_error:
                    logger.error(
                        f"Failed to create bucket '{bucket_name}': {create_error}"
                    )
                    return False
            else:
                logger.error(f"Error checking bucket '{bucket_name}': {e}")
                return False

    def test_write_read_operations(self, bucket_name: str) -> bool:
        """Test write and read operations."""
        try:
            # Test data
            test_key = f"test/test_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            test_data = {
                "timestamp": datetime.now().isoformat(),
                "test_message": "Hello from S3 tester!",
                "bucket": bucket_name,
                "operation": "write_test",
            }

            # Write test
            logger.info(f"Writing test file to {bucket_name}/{test_key}")
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=json.dumps(test_data, indent=2),
                ContentType="application/json",
            )
            logger.info("Write operation successful")

            # Read test
            logger.info(f"Reading test file from {bucket_name}/{test_key}")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=test_key)
            read_data = json.loads(response["Body"].read().decode("utf-8"))
            logger.info("Read operation successful")

            # Verify data
            if read_data == test_data:
                logger.info("Data verification successful")
                return True
            else:
                logger.error("Data verification failed")
                return False

        except Exception as e:
            logger.error(f"Write/read test failed: {e}")
            return False

    def list_objects(self, bucket_name: str, prefix: str = "") -> list:
        """List objects in bucket."""
        try:
            logger.info(
                f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'"
            )

            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            objects = []
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append(
                            {
                                "Key": obj["Key"],
                                "Size": obj["Size"],
                                "LastModified": obj["LastModified"].isoformat(),
                            }
                        )

            logger.info(f"Found {len(objects)} objects")
            return objects

        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def run_comprehensive_test(self) -> bool:
        """Run comprehensive S3 tests."""
        logger.info("Starting comprehensive S3 tests...")

        success = True

        # Test 1: Connection
        if not self.test_connection():
            success = False

        # Test 2: List buckets
        _ = self.list_buckets()

        # Test 3: Create buckets
        for bucket_name in [self.landing_bucket, self.data_bucket]:
            if not self.create_bucket_if_not_exists(bucket_name):
                success = False

        # Test 4: Write/Read operations
        for bucket_name in [self.landing_bucket, self.data_bucket]:
            if not self.test_write_read_operations(bucket_name):
                success = False

        # Test 5: List objects
        for bucket_name in [self.landing_bucket, self.data_bucket]:
            _ = self.list_objects(bucket_name)

        if success:
            logger.info("All S3 tests passed successfully!")
        else:
            logger.error("Some S3 tests failed!")

        return success


def main():
    """Main function to run S3 tests."""
    logger.info("Starting S3/MinIO connectivity tests...")

    tester = S3Tester()
    success = tester.run_comprehensive_test()

    if success:
        logger.info("S3 testing completed successfully")
        exit(0)
    else:
        logger.error("S3 testing failed")
        exit(1)


if __name__ == "__main__":
    main()
