#!/bin/bash

# Spark S3/MinIO ETL Job Runner Script
# This script runs the Spark ETL job with optimized configurations

set -e

# Default values
INPUT_PATH=""
OUTPUT_PATH=""
DATE_FILTER=""
OUTPUT_FORMAT="parquet"
SPARK_MASTER="local[*]"
DEPLOY_MODE="client"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --date-filter)
            DATE_FILTER="$2"
            shift 2
            ;;
        --output-format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --spark-master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        --deploy-mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0  [options]"
            echo ""
            echo "Optional arguments:"
            echo "  --date-filter    Date filter in YYYY-MM-DD format"
            echo "  --output-format  Output format: parquet, csv, json (default: parquet)"
            echo "  --spark-master   Spark master URL (default: local[*])"
            echo "  --deploy-mode    Deploy mode: client, cluster (default: client)"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

# Load environment variables if .env file exists
if [[ -f .env ]]; then
    echo "Loading environment variables from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Validate required environment variables
if [[ -z "$AWS_ENDPOINT" || -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: AWS environment variables are required:"
    echo "  AWS_ENDPOINT"
    echo "  AWS_ACCESS_KEY_ID"
    echo "  AWS_SECRET_ACCESS_KEY"
    exit 1
fi

# Set Spark configuration based on data size estimation
echo "Configuring Spark based on estimated data size..."

# Default configuration for medium datasets
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES="2"
NUM_EXECUTORS="4"
DRIVER_MEMORY="2g"
SHUFFLE_PARTITIONS="200"

# Check if we can estimate data size (basic heuristic)
if command -v aws &> /dev/null; then
    echo "Attempting to estimate data size..."
    # This would require AWS CLI configuration
    # DATA_SIZE=$(aws s3 ls --recursive $INPUT_PATH --summarize | grep "Total Size" | awk '{print $3}')
fi

echo "Starting Spark ETL job with configuration:"
echo "  Date Filter: ${DATE_FILTER:-"None"}"
echo "  Output Format: $OUTPUT_FORMAT"
echo "  Spark Master: $SPARK_MASTER"
echo "  Deploy Mode: $DEPLOY_MODE"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Executor Cores: $EXECUTOR_CORES"
echo "  Number of Executors: $NUM_EXECUTORS"
echo "  Driver Memory: $DRIVER_MEMORY"

# Build spark-submit command
SPARK_SUBMIT_CMD="spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode $DEPLOY_MODE \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --num-executors $NUM_EXECUTORS \
    --driver-memory $DRIVER_MEMORY \
    --driver-max-result-size 1g \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.hadoop.fs.s3a.endpoint=$AWS_ENDPOINT \
    --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
    --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.connection.timeout=200000 \
    --conf spark.hadoop.fs.s3a.connection.establish.timeout=40000 \
    --conf spark.hadoop.fs.s3a.connection.maximum=100 \
    --conf spark.hadoop.fs.s3a.attempts.maximum=10 \
    --conf spark.hadoop.fs.s3a.retry.limit=5 \
    --conf spark.hadoop.fs.s3a.retry.interval=500ms \
    --conf spark.hadoop.fs.s3a.multipart.size=104857600 \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf spark.hadoop.fs.s3a.block.size=134217728 \
    --conf spark.hadoop.fs.s3a.readahead.range=1048576 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark_s3_percentile_etl.py"

# Add optional arguments
if [[ -n "$DATE_FILTER" ]]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --date-filter $DATE_FILTER"
fi

SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --output-format $OUTPUT_FORMAT"

echo ""
echo "Executing Spark job..."
echo "Command: $SPARK_SUBMIT_CMD"
echo ""

# Execute the command
eval $SPARK_SUBMIT_CMD

# Check exit status
if [[ $? -eq 0 ]]; then
    echo ""
    echo "✅ Spark ETL job completed successfully!"
    echo "Results written to: $OUTPUT_PATH"
else
    echo ""
    echo "❌ Spark ETL job failed!"
    exit 1
fi