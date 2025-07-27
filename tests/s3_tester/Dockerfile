FROM python:3.11.14-slim

WORKDIR /app

# Install boto3 for S3 operations
RUN pip install --no-cache-dir boto3

COPY s3_tester.py .

CMD ["python", "s3_tester.py"]