FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Set working directory
WORKDIR /opt/spark/work-dir

# Switch to root to install packages
USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY spark_s3_percentile_etl.py .
COPY run_etl.sh .
COPY .env .

# Make scripts executable
RUN chmod +x run_etl.sh

# Download required JARs for S3 support
RUN wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch back to spark user
USER spark

# Default command
CMD ["bash"]