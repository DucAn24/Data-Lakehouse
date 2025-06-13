# ETL Pipeline Guide - Data Lakehouse

This comprehensive guide covers the complete ETL (Extract, Transform, Load) pipeline for the flood monitoring data lakehouse, implementing a medallion architecture (Bronze → Silver → Gold).

## 🏗️ Pipeline Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │     EXTRACT     │    │   TRANSFORM     │    │      LOAD       │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Flood APIs    │───▶│ • Kafka Streams │───▶│ • Bronze Layer  │───▶│ • MinIO Storage │
│ • Weather APIs  │    │ • API Polling   │    │ • Silver Layer  │    │ • Delta Tables  │
│ • Sensor Data   │    │ • File Ingestion│    │ • Gold Layer    │    │ • Hive Metastore│
│ • Historical DB │    │ • Real-time     │    │ • Data Quality  │    │ • Query Engines │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📂 Directory Structure

```
etl-pineline/
├── extract/                    # Data extraction scripts
│   ├── kafka_producers/        # Kafka producer scripts
│   ├── api_collectors/         # API data collection
│   ├── batch_extractors/       # Batch data extraction
│   └── streaming_extractors/   # Real-time data extraction
├── transform/                  # Data transformation scripts
│   ├── bronze_to_silver.py     # Silver layer transformation
│   ├── silver_to_gold.py       # Gold layer transformation
│   ├── data_quality/           # Data quality checks
│   └── utils/                  # Transformation utilities
└── load/                       # Data loading utilities
    ├── delta_loaders/          # Delta Lake loaders
    ├── batch_loaders/          # Batch loading scripts
    └── streaming_loaders/      # Streaming data loaders
```

## 🔄 Data Flow Layers

### Bronze Layer (Raw Data)
- **Purpose**: Store raw, unprocessed data exactly as received
- **Format**: JSON files partitioned by date
- **Location**: `s3a://data-lakehouse/bronze/`
- **Schema**: Flexible, preserves original structure
- **Sources**: Kafka topics, API responses, file uploads

### Silver Layer (Cleaned Data)
- **Purpose**: Cleaned, validated, and enriched data
- **Format**: Delta Lake tables with optimized schema
- **Location**: `s3a://data-lakehouse/silver/delta/`
- **Schema**: Structured, normalized, with data types enforced
- **Features**: ACID transactions, time travel, schema evolution

### Gold Layer (Business-Ready Data)
- **Purpose**: Aggregated, dimensional model for analytics
- **Format**: Star schema with dimension and fact tables
- **Location**: `s3a://data-lakehouse/gold/delta/`
- **Schema**: Optimized for BI tools and reporting
- **Features**: Pre-calculated metrics, analytical views

## ⚡ Pipeline Execution Flow

### 1. Continuous Extraction (Real-time)
```bash
# Kafka producers continuously ingest data
docker exec kafka kafka-console-producer --topic flood-warnings --bootstrap-server localhost:9092

# Streaming extractors pull from APIs
python etl-pineline/extract/api_collectors/flood_api_extractor.py
```

### 2. Bronze Layer Loading (Near Real-time)
```bash
# Spark Structured Streaming from Kafka to Bronze
python etl-pineline/load/streaming_loaders/kafka_to_bronze.py
```

### 3. Silver Layer Transformation (Hourly/Daily)
```bash
# Clean and structure data
./run_transformation_delta.sh
# or
python etl-pineline/transform/bronze_to_silver.py
```

### 4. Gold Layer Transformation (Daily)
```bash
# Create dimensional model
./run_gold_transformation.sh
# or
python etl-pineline/transform/silver_to_gold.py
```

## 📊 Data Quality Framework

### Quality Checks at Each Layer

**Bronze Layer:**
- ✅ Data freshness (last update time)
- ✅ Volume checks (record counts)
- ✅ Format validation (valid JSON)

**Silver Layer:**
- ✅ Schema validation
- ✅ Data type enforcement
- ✅ Null value handling
- ✅ Duplicate detection
- ✅ Referential integrity

**Gold Layer:**
- ✅ Business rule validation
- ✅ Aggregation accuracy
- ✅ Dimension consistency
- ✅ KPI calculations

## 🔧 Configuration Management

### Environment Variables
```bash
# MinIO/S3 Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minio_access_key
MINIO_SECRET_KEY=minio_secret_key
MINIO_BUCKET=data-lakehouse

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPICS=flood-warnings,flood-areas,water-levels,stations,historical-readings

# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

### Pipeline Configuration File
```yaml
# etl-config.yaml
pipeline:
  name: "flood-monitoring-etl"
  schedule: "hourly"
  
sources:
  flood_api:
    url: "https://environment.data.gov.uk/flood-monitoring/"
    endpoints:
      - "floods"
      - "stations"
      - "readings"
    rate_limit: 10  # requests per second
    
  kafka:
    topics:
      - "flood-warnings"
      - "water-levels"
    batch_size: 1000
    
transformations:
  bronze_to_silver:
    enabled: true
    schedule: "0 */1 * * *"  # hourly
    
  silver_to_gold:
    enabled: true
    schedule: "0 2 * * *"    # daily at 2 AM
    
quality_checks:
  enabled: true
  thresholds:
    max_null_percentage: 5
    min_records_per_day: 1000
    max_duplicate_percentage: 1
```

## 🚀 Running the Pipeline

### Option 1: Manual Execution
```bash
# 1. Start the environment
docker-compose up -d

# 2. Run Bronze to Silver transformation
./run_transformation_delta.sh

# 3. Run Silver to Gold transformation
./run_gold_transformation.sh

# 4. Verify data quality
python etl-pineline/transform/data_quality/quality_checker.py
```

### Option 2: Automated Execution (Cron/Airflow)
```bash
# Add to crontab for automated execution
# Run Silver transformation every hour
0 * * * * /path/to/run_transformation_delta.sh

# Run Gold transformation daily at 2 AM
0 2 * * * /path/to/run_gold_transformation.sh
```

## 📈 Monitoring and Alerting

### Key Metrics to Monitor
- **Data Freshness**: Time since last successful ingestion
- **Data Volume**: Record counts per source and layer
- **Data Quality**: Success rate of quality checks
- **Pipeline Performance**: Execution time and resource usage
- **Error Rates**: Failed transformations and their causes

### Monitoring Dashboard Queries
```sql
-- Data freshness check
SELECT 
    table_name,
    MAX(loaded_at) as last_update,
    COUNT(*) as record_count
FROM (
    SELECT 'bronze_stations' as table_name, processed_at as loaded_at FROM flood_monitoring.stations_delta
    UNION ALL
    SELECT 'silver_water_levels', loaded_at FROM flood_analytics_gold.fact_water_levels
) monitoring_data
GROUP BY table_name;

-- Quality metrics
SELECT 
    layer,
    table_name,
    record_count,
    null_count,
    (null_count * 100.0 / record_count) as null_percentage
FROM quality_checks
WHERE check_timestamp >= current_date - interval '7' day;
```

## 🛠️ Troubleshooting Common Issues

### Issue 1: Pipeline Failures
```bash
# Check Spark application logs
docker logs spark-master

# Check transformation logs
docker exec spark-master find /opt/bitnami/spark/logs -name "*.log" -exec tail -f {} +

# Verify Kafka topics
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092
```

### Issue 2: Data Quality Issues
```bash
# Run data quality checks manually
python etl-pineline/transform/data_quality/quality_checker.py --layer silver --verbose

# Check for duplicates
spark-sql -e "SELECT station_id, COUNT(*) FROM flood_monitoring.stations_delta GROUP BY station_id HAVING COUNT(*) > 1"
```

### Issue 3: Performance Problems
```bash
# Check cluster resources
docker stats

# Optimize Spark configuration
spark-submit \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    your_script.py
```

## 📚 Best Practices

### Development Guidelines
1. **Version Control**: All ETL scripts in Git with proper branching
2. **Testing**: Unit tests for transformation logic
3. **Documentation**: Code comments and pipeline documentation
4. **Error Handling**: Comprehensive exception handling and logging
5. **Idempotency**: Ensure transformations can be safely re-run

### Data Management
1. **Partitioning**: Partition large tables by date and relevant dimensions
2. **Compaction**: Regular Delta Lake table optimization
3. **Retention**: Implement data retention policies
4. **Backup**: Regular backups of critical datasets
5. **Security**: Proper access controls and data encryption

### Performance Optimization
1. **Caching**: Cache frequently accessed intermediate datasets
2. **Broadcast Joins**: Use broadcast joins for small dimension tables
3. **Column Pruning**: Select only required columns
4. **Predicate Pushdown**: Apply filters as early as possible
5. **Resource Tuning**: Optimize Spark executor and driver settings

## 🔗 Integration Points

### With ML Pipeline
```python
# ML engineers can access cleaned features
df_features = spark.table("flood_analytics_gold.fact_water_levels")
df_ml_ready = spark.table("flood_monitoring.ml_features_delta")
```

### With BI Tools
```sql
-- Superset can query analytical views
SELECT * FROM flood_analytics_gold.v_station_performance;
SELECT * FROM flood_analytics_gold.v_daily_trends;
```

### With Real-time Systems
```python
# Stream processing for alerts
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("RealTimeAlerts").getOrCreate()

# Read streaming data
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "water-levels") \
    .load()

# Process and trigger alerts
alerts = stream_df.filter(col("water_level") > threshold)
```

## 📊 Data Lineage

```
Raw APIs/Sensors
      ↓
 Kafka Topics (flood-warnings, water-levels, stations, historical-readings)
      ↓
Bronze Layer (JSON files, partitioned by date)
      ↓
Silver Layer (Delta tables with cleaned, typed data)
      ↓ 
Gold Layer (Star schema: dims + facts + analytical views)
      ↓
BI Tools (Superset) / ML Models / Real-time Alerts
```

## 🎯 Success Metrics

### Pipeline Health
- **Availability**: 99.5% uptime
- **Latency**: Bronze ingestion < 5 minutes, Silver transformation < 30 minutes
- **Accuracy**: Data quality checks passing > 99%
- **Completeness**: No missing data for > 95% of expected sources

### Business Impact
- **Time to Insight**: From raw data to dashboard < 1 hour
- **Data Freshness**: Real-time data available within 5 minutes
- **Query Performance**: Dashboard queries < 10 seconds
- **Cost Efficiency**: Storage and compute costs within budget

---

This ETL pipeline provides a robust, scalable foundation for your flood monitoring data lakehouse! 🌊📊 