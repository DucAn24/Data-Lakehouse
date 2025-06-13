# Transform Phase Guide - Data Lakehouse ETL Pipeline

This guide covers the **Transform** phase of the ETL pipeline, which processes data through the medallion architecture layers: Bronze → Silver → Gold. This phase is responsible for data cleaning, validation, enrichment, and creating analytical-ready datasets.

## 🎯 Transform Phase Overview

The Transform phase implements a medallion architecture with three distinct layers, each serving specific purposes in the data processing pipeline.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                               TRANSFORM PHASE                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Bronze Layer          Silver Layer           Gold Layer                        │
│  ┌─────────────┐      ┌─────────────┐       ┌─────────────┐                   │
│  │ Raw JSON    │─────▶│ Clean Delta │──────▶│ Star Schema │                   │
│  │ - Kafka     │      │ - Validated │       │ - Dims      │                   │
│  │ - Files     │      │ - Typed     │       │ - Facts     │                   │
│  │ - Streams   │      │ - Enriched  │       │ - Views     │                   │
│  │ - Archives  │      │ - Quality   │       │ - KPIs      │                   │
│  └─────────────┘      └─────────────┘       └─────────────┘                   │
│                                                                                 │
│  Features:             Features:             Features:                          │
│  • Schema-on-read     • ACID transactions   • Pre-aggregated                   │
│  • Append-only        • Time travel         • Optimized joins                  │
│  • Partitioned        • Data quality        • BI-ready                         │
│  • Compressed         • ML features         • Fast queries                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 📂 Transform Directory Structure

```
etl-pineline/transform/
├── bronze_to_silver.py               # Main Silver transformation
├── silver_to_gold.py                 # Main Gold transformation
├── data_quality/
│   ├── quality_checker.py           # Data quality validation
│   ├── schema_validator.py          # Schema validation
│   ├── anomaly_detector.py          # Anomaly detection
│   └── quality_rules.yaml           # Quality rule definitions
├── utils/
│   ├── spark_utils.py               # Spark utility functions
│   ├── delta_utils.py               # Delta Lake utilities
│   ├── transformation_utils.py      # Common transformations
│   └── logging_utils.py             # Logging configuration
├── streaming/
│   ├── streaming_bronze.py          # Real-time Bronze ingestion
│   ├── streaming_silver.py          # Real-time Silver updates
│   └── checkpoint_manager.py        # Streaming checkpoints
└── config/
    ├── transformation_config.yaml   # Transformation settings
    ├── schema_definitions.yaml      # Schema definitions
    └── quality_thresholds.yaml      # Quality check thresholds
```

## 🥉 Bronze Layer Transformation

### Purpose and Characteristics

**Bronze Layer (Raw Data)**
- **Purpose**: Store raw, unprocessed data exactly as received
- **Format**: JSON files, preserving original structure
- **Schema**: Schema-on-read, flexible structure
- **Storage**: Partitioned by date for efficient queries
- **Quality**: Minimal validation, focus on ingestion speed

### Bronze Layer Processing

The Bronze layer primarily involves:
1. **Data Ingestion**: From Kafka topics to Delta Lake
2. **Partitioning**: By date and source for optimal performance
3. **Compression**: GZIP compression for storage efficiency
4. **Deduplication**: Basic duplicate removal
5. **Metadata Addition**: Ingestion timestamps and source tracking

### Sample Bronze Transformation

```python
def ingest_to_bronze(spark, kafka_topic, bronze_path):
    """Ingest streaming data from Kafka to Bronze layer"""
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", kafka_topic) \
        .load()
    
    # Parse JSON and add metadata
    processed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        current_timestamp().alias("ingestion_time"),
        date_format(current_timestamp(), "yyyy-MM-dd").alias("date_str")
    )
    
    # Write to Bronze Delta table
    query = processed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{bronze_path}/_checkpoint") \
        .partitionBy("date_str") \
        .start(bronze_path)
    
    return query
```

## 🥈 Silver Layer Transformation

### Purpose and Characteristics

**Silver Layer (Clean Data)**
- **Purpose**: Cleaned, validated, and enriched data
- **Format**: Delta Lake tables with enforced schema
- **Schema**: Strongly typed, normalized structure
- **Quality**: Comprehensive validation and cleansing
- **Features**: ACID transactions, time travel, schema evolution

### Key Silver Transformations

#### 1. Data Cleaning and Validation

```python
def clean_station_data(df):
    """Clean and validate station data"""
    
    return df.select(
        # Extract and clean station ID
        regexp_extract(col("station.@id"), r".*/(.+)$", 1).alias("station_id"),
        
        # Clean and validate coordinates
        col("station.lat").cast(DoubleType()).alias("latitude"),
        col("station.long").cast(DoubleType()).alias("longitude"),
        
        # Standardize text fields
        trim(upper(col("station.label"))).alias("station_name"),
        trim(upper(col("station.riverName"))).alias("river_name"),
        trim(upper(col("station.town"))).alias("town"),
        
        # Type conversions with null handling
        coalesce(col("station.easting").cast(IntegerType()), lit(0)).alias("easting"),
        coalesce(col("station.northing").cast(IntegerType()), lit(0)).alias("northing"),
        
        # Metadata
        current_timestamp().alias("processed_at")
    ).filter(
        # Data quality filters
        col("station_id").isNotNull() &
        col("latitude").between(-90, 90) &
        col("longitude").between(-180, 180)
    )
```

#### 2. Feature Engineering for ML

```python
def create_time_series_features(df):
    """Create time series features for machine learning"""
    
    window_spec = Window.partitionBy("station_id").orderBy("reading_datetime")
    
    return df.withColumn(
        # Lag features
        "previous_level", lag("water_level", 1).over(window_spec)
    ).withColumn(
        "level_change", col("water_level") - col("previous_level")
    ).withColumn(
        # Rolling statistics (3-hour window)
        "rolling_avg_3h", avg("water_level").over(
            window_spec.rowsBetween(-2, 2)
        )
    ).withColumn(
        "rolling_std_3h", stddev("water_level").over(
            window_spec.rowsBetween(-2, 2)
        )
    ).withColumn(
        # Time-based features
        "hour", hour("reading_datetime")
    ).withColumn(
        "day_of_week", dayofweek("reading_datetime")
    ).withColumn(
        "is_weekend", when(dayofweek("reading_datetime").isin(1, 7), 1).otherwise(0)
    )
```

#### 3. Data Quality Checks

```python
def validate_silver_data(df, table_name):
    """Perform comprehensive data quality validation"""
    
    # Record count validation
    record_count = df.count()
    if record_count == 0:
        raise ValueError(f"No records found in {table_name}")
    
    # Null check for critical fields
    critical_fields = ["station_id", "water_level", "reading_datetime"]
    for field in critical_fields:
        null_count = df.filter(col(field).isNull()).count()
        null_percentage = (null_count / record_count) * 100
        
        if null_percentage > 5:  # Threshold: 5%
            logger.warning(f"High null percentage in {field}: {null_percentage:.2f}%")
    
    # Duplicate check
    duplicate_count = df.groupBy("station_id", "reading_datetime").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate records in {table_name}")
    
    # Range validation for water levels
    invalid_levels = df.filter((col("water_level") < -10) | (col("water_level") > 50)).count()
    if invalid_levels > 0:
        logger.warning(f"Found {invalid_levels} records with invalid water levels")
    
    return True
```

### Running Silver Transformation

```bash
# Manual execution
python etl-pineline/transform/bronze_to_silver.py

# Using the shell script
./run_transformation_delta.sh

# With specific configuration
python etl-pineline/transform/bronze_to_silver.py --config config/silver_config.yaml
```

## 🥇 Gold Layer Transformation

### Purpose and Characteristics

**Gold Layer (Analytics-Ready Data)**
- **Purpose**: Business-ready data optimized for analytics and BI
- **Format**: Star schema with dimensions and facts
- **Schema**: Denormalized for query performance
- **Quality**: Business rule validation and KPI calculations
- **Features**: Pre-aggregated metrics, analytical views

### Star Schema Design

#### Dimension Tables

**1. dim_time - Calendar Dimension**
```sql
CREATE TABLE flood_analytics_gold.dim_time (
    time_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    month_name STRING,
    day_name STRING,
    is_weekend BOOLEAN,
    season STRING
)
```

**2. dim_stations - Station Master Data**
```sql
CREATE TABLE flood_analytics_gold.dim_stations (
    station_key STRING PRIMARY KEY,
    station_id STRING,
    station_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    river_name STRING,
    town STRING,
    region STRING,
    flood_risk_category STRING,
    max_on_record_value DOUBLE
)
```

#### Fact Tables

**1. fact_water_levels - Detailed Measurements**
```sql
CREATE TABLE flood_analytics_gold.fact_water_levels (
    station_key STRING,
    time_key INT,
    water_level DOUBLE,
    reading_datetime TIMESTAMP,
    is_high_risk INT,
    is_low_risk INT,
    risk_score DOUBLE,
    year INT,
    month INT
) PARTITIONED BY (year, month)
```

**2. fact_daily_summary - Pre-aggregated Daily Metrics**
```sql
CREATE TABLE flood_analytics_gold.fact_daily_summary (
    station_key STRING,
    time_key INT,
    reading_count BIGINT,
    avg_water_level DOUBLE,
    max_water_level DOUBLE,
    min_water_level DOUBLE,
    stddev_water_level DOUBLE,
    high_risk_readings BIGINT,
    avg_risk_score DOUBLE
) PARTITIONED BY (year, month)
```

### Gold Layer Analytical Views

#### 1. Station Performance View

```sql
CREATE VIEW flood_analytics_gold.v_station_performance AS
SELECT 
    s.station_name,
    s.river_name,
    s.region,
    s.flood_risk_category,
    COUNT(f.water_level) as total_readings,
    AVG(f.water_level) as avg_water_level,
    MAX(f.water_level) as max_water_level,
    STDDEV(f.water_level) as water_level_volatility,
    AVG(f.risk_score) as avg_risk_score,
    SUM(f.is_high_risk) as high_risk_events
FROM flood_analytics_gold.fact_water_levels f
JOIN flood_analytics_gold.dim_stations s ON f.station_key = s.station_key
GROUP BY s.station_name, s.river_name, s.region, s.flood_risk_category
```

#### 2. Daily Trends View

```sql
CREATE VIEW flood_analytics_gold.v_daily_trends AS
SELECT 
    t.full_date,
    t.season,
    t.is_weekend,
    s.region,
    ds.reading_count,
    ds.avg_water_level,
    ds.high_risk_readings
FROM flood_analytics_gold.fact_daily_summary ds
JOIN flood_analytics_gold.dim_time t ON ds.time_key = t.time_key
JOIN flood_analytics_gold.dim_stations s ON ds.station_key = s.station_key
```

### Running Gold Transformation

```bash
# Manual execution
python etl-pineline/transform/silver_to_gold.py

# Using the shell script
./run_gold_transformation.sh

# With custom date range
python etl-pineline/transform/silver_to_gold.py --start-date 2025-01-01 --end-date 2025-01-31
```

## 📊 Data Quality Framework

### Quality Checks by Layer

#### Bronze Layer Quality Checks
- ✅ **Data Freshness**: Ensure data is recent
- ✅ **Volume Validation**: Expected record counts
- ✅ **Format Validation**: Valid JSON structure
- ✅ **Source Validation**: Correct topic/source

#### Silver Layer Quality Checks
- ✅ **Schema Validation**: Correct data types
- ✅ **Null Value Checks**: Critical fields not null
- ✅ **Range Validation**: Values within expected ranges
- ✅ **Duplicate Detection**: No unexpected duplicates
- ✅ **Referential Integrity**: Valid foreign keys

#### Gold Layer Quality Checks
- ✅ **Business Rule Validation**: Domain-specific rules
- ✅ **Aggregation Accuracy**: Correct calculations
- ✅ **Dimension Consistency**: Consistent dimension data
- ✅ **KPI Validation**: Key metrics within bounds

### Quality Configuration

```yaml
# quality_thresholds.yaml
quality_checks:
  bronze:
    max_null_percentage: 10
    min_records_per_hour: 100
    max_duplicate_percentage: 1
    
  silver:
    max_null_percentage: 2
    min_records_per_day: 1000
    max_duplicate_percentage: 0.1
    water_level_range: [-10, 50]
    
  gold:
    max_null_percentage: 0
    aggregation_tolerance: 0.01
    kpi_thresholds:
      avg_risk_score: [0, 100]
      station_count: [100, 10000]
```

## 🔧 Configuration Management

### Transformation Configuration

```yaml
# transformation_config.yaml
spark:
  app_name: "FloodDataTransformation"
  master: "spark://spark-master:7077"
  driver_memory: "2g"
  executor_memory: "2g"
  sql_adaptive_enabled: true

sources:
  bronze_path: "s3a://data-lakehouse/bronze"
  silver_path: "s3a://data-lakehouse/silver/delta"
  gold_path: "s3a://data-lakehouse/gold/delta"

transformations:
  bronze_to_silver:
    enabled: true
    batch_size: 10000
    partitions: ["year", "month"]
    
  silver_to_gold:
    enabled: true
    create_views: true
    optimize_tables: true
    
data_quality:
  enabled: true
  fail_on_error: false
  report_path: "s3a://data-lakehouse/quality-reports"
```

### Schema Definitions

```yaml
# schema_definitions.yaml
schemas:
  flood_warning:
    fields:
      - name: "flood_id"
        type: "string"
        nullable: false
      - name: "severity"
        type: "string"
        nullable: false
        values: ["Severe Flood Warning", "Flood Warning", "Flood Alert"]
      - name: "description"
        type: "string"
        nullable: true
        
  water_level:
    fields:
      - name: "station_id"
        type: "string"
        nullable: false
      - name: "water_level"
        type: "double"
        nullable: false
        range: [-10.0, 50.0]
      - name: "reading_datetime"
        type: "timestamp"
        nullable: false
```

## 🚀 Performance Optimization

### Spark Optimization Techniques

#### 1. Partitioning Strategy

```python
# Partition by date for time-based queries
df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .format("delta") \
    .save(output_path)

# Repartition for even distribution
df.repartition(10, "station_id") \
    .write \
    .format("delta") \
    .save(output_path)
```

#### 2. Caching Strategy

```python
# Cache frequently accessed DataFrames
stations_df.cache()

# Persist with appropriate storage level
historical_df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
stations_df.unpersist()
```

#### 3. Broadcast Joins

```python
# Broadcast small dimension tables
stations_broadcast = broadcast(stations_df)

result = large_fact_df.join(
    stations_broadcast, 
    "station_id", 
    "left"
)
```

#### 4. Delta Lake Optimizations

```python
# Optimize tables after writes
spark.sql("OPTIMIZE flood_analytics_gold.fact_water_levels")

# Z-order for better query performance
spark.sql("""
OPTIMIZE flood_analytics_gold.fact_water_levels
ZORDER BY (station_key, reading_datetime)
""")

# Vacuum old files
spark.sql("VACUUM flood_analytics_gold.fact_water_levels RETAIN 168 HOURS")
```

### Configuration Tuning

```python
# Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Memory optimization
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Delta Lake settings
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
```

## 📈 Monitoring and Alerting

### Key Metrics to Monitor

1. **Transformation Performance**
   - Processing time per layer
   - Record throughput (records/second)
   - Resource utilization (CPU, memory)

2. **Data Quality Metrics**
   - Quality check pass/fail rates
   - Data freshness (lag time)
   - Schema drift detection

3. **Delta Lake Metrics**
   - Table size and growth
   - File count and optimization status
   - Query performance

### Monitoring Queries

```sql
-- Transformation performance
SELECT 
    table_name,
    MAX(loaded_at) as last_update,
    COUNT(*) as record_count,
    SUM(CASE WHEN loaded_at >= current_date - 1 THEN 1 ELSE 0 END) as recent_records
FROM (
    SELECT 'bronze_stations' as table_name, processed_at as loaded_at FROM flood_monitoring.stations_delta
    UNION ALL
    SELECT 'silver_water_levels', loaded_at FROM flood_analytics_gold.fact_water_levels
) monitoring_data
GROUP BY table_name;

-- Data quality summary
SELECT 
    layer,
    table_name,
    check_name,
    status,
    error_count,
    check_timestamp
FROM data_quality.quality_results 
WHERE check_timestamp >= current_date - 7
ORDER BY check_timestamp DESC;
```

## 🚨 Error Handling and Recovery

### Common Issues and Solutions

#### 1. Schema Evolution Errors

```python
# Handle schema changes gracefully
try:
    df.write.format("delta").mode("append").save(path)
except AnalysisException as e:
    if "schema" in str(e).lower():
        # Enable schema evolution
        df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(path)
```

#### 2. Data Quality Failures

```python
def handle_quality_failure(df, table_name, quality_check):
    """Handle data quality check failures"""
    
    # Log the failure
    logger.error(f"Quality check failed for {table_name}: {quality_check}")
    
    # Quarantine bad records
    bad_records = df.filter(~quality_check)
    bad_records.write \
        .mode("append") \
        .format("delta") \
        .save(f"s3a://data-lakehouse/quarantine/{table_name}")
    
    # Continue with good records
    good_records = df.filter(quality_check)
    return good_records
```

#### 3. Resource Management

```python
def safe_transformation(func, *args, **kwargs):
    """Wrapper for safe transformation execution"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        # Clean up resources
        spark.catalog.clearCache()
        # Retry with smaller batch size
        if 'batch_size' in kwargs:
            kwargs['batch_size'] = kwargs['batch_size'] // 2
            return func(*args, **kwargs)
        raise
```

## 🔗 Integration Points

### With Extract Phase

```python
# Read from Kafka topics populated by Extract phase
bronze_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flood-warnings,stations,water-levels") \
    .load()
```

### With ML Pipeline

```python
# Provide ML-ready features
ml_features_df = spark.table("flood_monitoring.ml_features_delta")

# Feature store integration
from feast import FeatureStore
fs = FeatureStore(repo_path=".")
fs.apply([station_features, water_level_features])
```

### With BI Tools

```python
# Create materialized views for BI tools
spark.sql("""
CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_summary AS
SELECT 
    region,
    date,
    avg(water_level) as avg_level,
    max(water_level) as max_level,
    sum(is_high_risk) as risk_events
FROM flood_analytics_gold.v_daily_trends
GROUP BY region, date
""")
```

## 🎯 Best Practices

### Development Guidelines

1. **Incremental Processing**: Process only new/changed data
2. **Idempotency**: Ensure transformations can be safely re-run
3. **Error Handling**: Comprehensive exception handling and logging
4. **Testing**: Unit tests for transformation logic
5. **Documentation**: Clear documentation for business logic

### Production Deployment

1. **Environment Separation**: Dev/Test/Prod environments
2. **CI/CD Integration**: Automated testing and deployment
3. **Monitoring**: Comprehensive monitoring and alerting
4. **Backup**: Regular backups of critical datasets
5. **Security**: Proper access controls and encryption

---

This Transform phase provides a robust, scalable foundation for processing flood monitoring data through your data lakehouse! 🔄📊 