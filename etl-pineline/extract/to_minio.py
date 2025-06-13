import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

# Kafka topics to consume from
KAFKA_TOPICS = [
    'flood-warnings',
    'flood-areas', 
    'water-levels',
    'stations',
    'historical-readings'
]

# MinIO/S3 paths for bronze layer
BRONZE_BASE_PATH = "s3a://data-lakehouse/bronze"
CHECKPOINT_BASE_PATH = "s3a://data-lakehouse/checkpoints"

def create_spark_session():
    """Create Spark session with required configurations for Kafka and MinIO"""
    try:
        spark = SparkSession.builder \
            .appName("FloodDataStreaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.checkpointLocation.deleteOnStop", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def read_kafka_stream(spark, topics):
    """Read streaming data from Kafka topics"""
    try:
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .option("maxOffsetsPerTrigger", "500") \
            .option("kafka.max.poll.records", "100") \
            .load()
        
        logger.info(f"Kafka stream created for topics: {topics}")
        return kafka_stream
    except Exception as e:
        logger.error(f"Failed to create Kafka stream: {e}")
        raise

def process_kafka_messages(df):
    """Process Kafka messages and extract JSON data"""
    # Convert binary data to string and add metadata
    processed_df = df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("json_data")
    ).withColumn(
        "processing_timestamp", current_timestamp()
    ).withColumn(
        "year", year(col("kafka_timestamp"))
    ).withColumn(
        "month", month(col("kafka_timestamp"))
    ).withColumn(
        "day", dayofmonth(col("kafka_timestamp"))
    )
    
    return processed_df

def write_to_bronze_layer(df, topic_name):
    """Write streaming data to MinIO bronze layer"""
    try:
        # Define the output path with partitioning
        output_path = f"{BRONZE_BASE_PATH}/{topic_name}"
        # Store checkpoints in MinIO so they persist across container restarts
        checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{topic_name}"
        
        logger.info(f"Using checkpoint path: {checkpoint_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", output_path) \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info(f"Started streaming to {output_path}")
        return query
    except Exception as e:
        logger.error(f"Failed to write stream for topic {topic_name}: {e}")
        raise

def write_topic_specific_stream(spark, kafka_df, topic_name):
    """Write data for a specific topic to bronze layer"""
    # Filter for specific topic (use "topic" not "kafka_topic" before processing)
    topic_df = kafka_df.filter(col("topic") == topic_name)
    
    # Process the data
    processed_df = process_kafka_messages(topic_df)
    
    # Write to bronze layer
    return write_to_bronze_layer(processed_df, topic_name)

def cleanup_checkpoints():
    """Clean up existing checkpoints when necessary"""
    import shutil
    import os
    
    checkpoint_base = "/tmp/spark-checkpoints"

    
    if os.path.exists(checkpoint_base):
        try:
            shutil.rmtree(checkpoint_base)
            os.makedirs(checkpoint_base, exist_ok=True)
            logger.info("Cleaned up all existing checkpoints")
        except Exception as e:
            logger.warning(f"Could not clean checkpoints: {e}")
    else:
        logger.info("No existing checkpoints found")

def verify_bronze_path(spark):
    """Verify bronze path exists in MinIO"""
    try:
        # Simple verification that we can access the path
        logger.info(f"Using existing bronze path: {BRONZE_BASE_PATH}")
        
    except Exception as e:
        logger.warning(f"Could not verify bronze path: {e}")

def main():
    logger.info("Starting Flood Data Streaming to Bronze Layer")
    
    try:
        # Only force cleanup if you're having serialization issues
        cleanup_checkpoints()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Verify bronze path exists
        verify_bronze_path(spark)
        
        # Read from Kafka
        kafka_df = read_kafka_stream(spark, KAFKA_TOPICS)
        
        # Create streaming queries for each topic
        streaming_queries = []
        
        for topic in KAFKA_TOPICS:
            logger.info(f"Setting up streaming for topic: {topic}")
            try:
                query = write_topic_specific_stream(spark, kafka_df, topic)
                streaming_queries.append(query)
            except Exception as e:
                logger.error(f"Failed to set up streaming for topic {topic}: {e}")
                continue
        
        logger.info(f"Started {len(streaming_queries)} streaming queries")
        
        # Monitor streaming queries
        import time
        while True:
            time.sleep(30)  # Check every 30 seconds
            
            active_queries = [q for q in streaming_queries if q.isActive]
            logger.info(f"Active queries: {len(active_queries)}/{len(streaming_queries)}")
            
            # Show progress for each query
            for i, query in enumerate(streaming_queries):
                if query.isActive:
                    progress = query.lastProgress
                    if progress:
                        topic_name = KAFKA_TOPICS[i]
                        num_input_rows = progress.get('inputRowsPerSecond', 0)
                        num_processed_rows = progress.get('numInputRows', 0)
                        logger.info(f"Topic '{topic_name}': {num_processed_rows} rows processed, {num_input_rows} rows/sec")
                    else:
                        logger.info(f"Topic '{KAFKA_TOPICS[i]}': No progress data yet")
                else:
                    logger.error(f"Query for topic '{KAFKA_TOPICS[i]}' is not active!")
            
            if not active_queries:
                logger.error("All streaming queries stopped!")
                break
            
    except Exception as e:
        logger.error(f"Error in streaming application: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()

# docker exec -it spark-master spark-submit /opt/bitnami/spark/scripts/to_minio.py