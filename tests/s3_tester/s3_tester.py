import os
import time
import boto3
from botocore.exceptions import ClientError

def create_s3_bucket_if_not_exists(s3_client,bucket_name):
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
            return True
        except ClientError as e:
            # If the error code is 404, the bucket does not exist, so we can create it
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                print(f"✅ Bucket '{bucket_name}' does not exist. Attempting to create...")

                try:
                    s3_client.create_bucket(
                            Bucket=bucket_name
                        )
                    print(f"✅ Bucket '{bucket_name}' created successfully.")
                    return True
                except ClientError as ce:
                    print(f"❌ Error creating bucket '{bucket_name}': {ce}")
                    return False
            elif error_code == 403:
                print(f"❌ Access denied to bucket '{bucket_name}'. Check your AWS credentials and permissions.")
                return False
            else:
                print(f"❌ An unexpected error occurred: {e}")
                return False

def main():
    # Get environment variables
    endpoint = os.getenv('AWS_ENDPOINT')
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('AWS_S3_BUCKET')

    print(f"Testing S3 connection to: {endpoint}")
    print(f"Using bucket: {bucket_name}")

    # Create S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3_resource = boto3.resource('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Test connection and bucket
    try:
        #create bucket if not exists
        if not create_s3_bucket_if_not_exists(s3,bucket_name):
            raise Exception (f'Error creating bucket {bucket_name}')

        # Test upload/download
        test_key = "connection-test.txt"
        s3.put_object(Bucket=bucket_name, Key=test_key, Body=b"test content")
        print(f"✅ Wrote test object: {test_key}")

        response = s3.get_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ Read test content: {response['Body'].read().decode()}")

        #clean up bucket
        bucket = s3_resource.Bucket(bucket_name) # type: ignore
        bucket.objects.all().delete()
        print(f"✅ Bucket '{bucket_name}' cleared")
        s3.delete_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' deleted")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ Bucket '{bucket_name}' does not exist")
        else:
            print(f"❌ Connection failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    # Add small delay to ensure MinIO is ready
    time.sleep(3)
    main()