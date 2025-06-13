# Extract Phase Guide - Data Lakehouse ETL Pipeline

This guide covers the **Extract** phase of the ETL pipeline, which is responsible for collecting data from various sources and ingesting it into the data lakehouse system.

## 🎯 Extract Phase Overview

The Extract phase collects flood monitoring data from multiple sources and streams it into Kafka topics for processing. This phase handles both real-time and batch data ingestion.

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXTRACT PHASE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Data Sources           Extractors              Kafka Topics    │
│  ┌─────────────┐       ┌─────────────┐        ┌─────────────┐   │
│  │ Flood API   │──────▶│ API Poller  │───────▶│ flood-warn  │   │
│  │ Weather API │       │             │        │ stations    │   │
│  │ Sensor Data │       │ File Reader │        │ water-level │   │
│  │ Historical  │       │ Stream Proc │        │ historical  │   │
│  │ Databases   │       │ Kafka Prod  │        │ flood-areas │   │
│  └─────────────┘       └─────────────┘        └─────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 📂 Extract Directory Structure

```
etl-pineline/extract/
├── api_collectors/
│   ├── flood_api_extractor.py      # UK Flood Monitoring API
│   ├── weather_api_extractor.py    # Weather data integration
│   └── sensor_data_collector.py    # IoT sensor data collection
├── batch_extractors/
│   ├── csv_file_extractor.py       # CSV file processing
│   ├── database_extractor.py       # Historical database queries
│   └── archive_processor.py        # Historical archive processing
├── streaming_extractors/
│   ├── kafka_stream_processor.py   # Kafka stream processing
│   ├── websocket_listener.py       # Real-time websocket data
│   └── mqtt_subscriber.py          # IoT MQTT data streams
├── kafka_producers/
│   ├── base_producer.py           # Base Kafka producer class
│   ├── flood_producer.py          # Flood data producer
│   └── sensor_producer.py         # Sensor data producer
└── utils/
    ├── rate_limiter.py            # API rate limiting
    ├── data_validator.py          # Data validation utilities
    └── config_manager.py          # Configuration management
```

## 🔄 Data Sources and Extraction Methods

### 1. UK Government Flood Monitoring API

**Primary Source**: https://environment.data.gov.uk/flood-monitoring/

**Endpoints Extracted:**
- `/floods` - Current flood warnings
- `/stations` - Monitoring station metadata
- `/flood-areas` - Flood area boundaries
- `/stations/{id}/readings` - Water level readings

**Extraction Method**: RESTful API polling with rate limiting
**Frequency**: Every 15 minutes for warnings, hourly for readings
**Format**: JSON responses
**Kafka Topics**: `flood-warnings`, `stations`, `flood-areas`, `water-levels`

### 2. Historical Water Level Data

**Source**: Station historical readings API
**Extraction Method**: Batch API calls with date range parameters
**Frequency**: Daily backfill for past 7 days
**Format**: JSON time series data
**Kafka Topic**: `historical-readings`

### 3. Real-time Sensor Data (Future Enhancement)

**Source**: IoT sensors, MQTT streams
**Extraction Method**: MQTT subscription, WebSocket connections
**Frequency**: Real-time streaming
**Format**: JSON sensor readings
**Kafka Topics**: `sensor-data`, `iot-readings`

## 🛠️ Key Extractor Components

### 1. API to Kafka Extractor (`extract/to_kafka.py`)

Your main extractor that fetches data from UK flood monitoring API and publishes to Kafka:

**Features:**
- ✅ Fetches from all API endpoints (floods, stations, readings, areas)
- ✅ Publishes to appropriate Kafka topics
- ✅ Saves data locally for backup (api-data directory)
- ✅ 15-minute extraction cycles
- ✅ Proper error handling and retry logic
- ✅ Rate limiting to respect API limits

**Usage:**
```bash
# Run the extractor
python etl-pineline/extract/to_kafka.py

# Check Kafka topics are receiving data
docker exec kafka kafka-console-consumer \
  --topic flood-warnings \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

**Kafka Topics Created:**
- `flood-warnings` - Current flood alerts
- `stations` - Monitoring station metadata  
- `water-levels` - Latest water level readings
- `flood-areas` - Flood area boundaries
- `historical-readings` - Historical water level data

### 2. Kafka to Bronze Layer (`extract/to_minio.py`)

Spark Structured Streaming job that ingests data from Kafka to MinIO bronze layer:

**Features:**
- ✅ Reads from multiple Kafka topics simultaneously
- ✅ Partitions data by date for efficient querying
- ✅ Stores raw JSON in MinIO bronze layer
- ✅ Proper checkpoint management for fault tolerance
- ✅ Configurable for different environments

**Usage:**
```bash
# Run with Spark
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  /opt/bitnami/spark/extract/to_minio.py
```

**Output Structure:**
```
s3a://data-lakehouse/bronze/
├── flood-warnings/date_str=2025-01-13/
├── stations/date_str=2025-01-13/
├── water-levels/date_str=2025-01-13/
├── flood-areas/date_str=2025-01-13/
└── historical-readings/date_str=2025-01-13/
```

## 🔄 Your Current Data Flow

```
UK Flood API → to_kafka.py → Kafka Topics → to_minio.py → MinIO Bronze Layer
```

This architecture provides excellent separation of concerns:
- **to_kafka.py**: Handles API rate limiting, data validation, and Kafka publishing
- **to_minio.py**: Handles scalable streaming ingestion and storage partitioning

## 📊 Data Flow and Message Format

### Kafka Message Structure

All extracted data follows a consistent message format:

```json
{
  "extraction_timestamp": "2025-01-13T10:30:00Z",
  "source": "flood-monitoring-api",
  "api_endpoint": "floods",
  "data": {
    "@id": "http://environment.data.gov.uk/flood-monitoring/id/floods/12345",
    "description": "Property flooding is expected",
    "eaAreaName": "Thames",
    "floodArea": {
      "@id": "http://environment.data.gov.uk/flood-monitoring/id/floodAreas/062FWF12Tewkesbury",
      "county": "Gloucestershire",
      "description": "River Severn at Tewkesbury"
    },
    "isTidal": false,
    "message": "River levels remain high...",
    "severity": "Alert",
    "severityLevel": 2,
    "timeMessageChanged": "2025-01-13T09:15:00Z",
    "timeRaised": "2025-01-13T08:30:00Z"
  }
}
```

### Topic Partitioning Strategy

- **Key-based partitioning**: Uses meaningful IDs (station_id, flood_id) as message keys
- **Benefits**: Ensures related messages go to same partition, maintaining order
- **Load balancing**: Even distribution across partitions

## 🚀 Running the Extract Phase

### Option 1: Manual Execution

```bash
# Start Kafka (if not already running)
docker-compose up -d kafka zookeeper

# Run flood API extractor
cd etl-pineline/extract/api_collectors
python flood_api_extractor.py

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor messages
docker exec kafka kafka-console-consumer \
  --topic flood-warnings \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### Option 2: Scheduled Extraction

```bash
# Set environment for scheduled mode
export EXTRACTION_MODE=scheduled
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run in background
nohup python etl-pineline/extract/api_collectors/flood_api_extractor.py > extraction.log 2>&1 &
```

### Option 3: Docker Container

```bash
# Build extractor container
docker build -t flood-extractor etl-pineline/extract/

# Run with environment variables
docker run -d \
  --network data-lakehouse-network \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e EXTRACTION_MODE=scheduled \
  flood-extractor
```

## 📈 Monitoring and Alerting

### Key Metrics to Monitor

1. **Extraction Rate**: Messages per minute by topic
2. **API Response Time**: Latency of external API calls
3. **Error Rate**: Failed extractions and API errors
4. **Data Freshness**: Time since last successful extraction
5. **Kafka Producer Metrics**: Message send success/failure rates

### Monitoring Queries

```bash
# Check topic message counts
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic flood-warnings

# Monitor consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

### Alerting Thresholds

- **API Failures**: Alert if >5% of API calls fail in 15 minutes
- **Data Lag**: Alert if no new data for >30 minutes during business hours
- **Kafka Errors**: Alert on any producer connection failures
- **Rate Limit**: Alert if hitting API rate limits

## 🛠️ Configuration Management

### Environment Variables

```bash
# Core Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
EXTRACTION_MODE=scheduled  # or "full"

# API Configuration
FLOOD_API_BASE_URL=https://environment.data.gov.uk/flood-monitoring/
API_RATE_LIMIT=60  # requests per minute
API_TIMEOUT=30     # seconds

# Kafka Producer Configuration
KAFKA_ACKS=all
KAFKA_RETRIES=3
KAFKA_COMPRESSION=gzip
KAFKA_BATCH_SIZE=16384
KAFKA_LINGER_MS=10

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT="%(asctime)s - %(levelname)s - %(message)s"
```

### Configuration File Example

```yaml
# extract-config.yaml
extraction:
  mode: scheduled
  
sources:
  flood_api:
    base_url: "https://environment.data.gov.uk/flood-monitoring/"
    rate_limit: 60
    timeout: 30
    endpoints:
      floods:
        schedule: "*/15 * * * *"  # Every 15 minutes
        topic: "flood-warnings"
        limit: 1000
      stations:
        schedule: "0 */6 * * *"   # Every 6 hours
        topic: "stations"
        limit: 5000
      readings:
        schedule: "0 * * * *"     # Every hour
        topic: "water-levels"
        limit: 2000

kafka:
  bootstrap_servers: "localhost:9092"
  producer:
    acks: "all"
    retries: 3
    compression_type: "gzip"
    max_request_size: 20971520

logging:
  level: "INFO"
  format: "%(asctime)s - %(levelname)s - %(message)s"
```

## 🚨 Error Handling and Recovery

### Common Issues and Solutions

**1. API Rate Limiting**
```python
# Implement exponential backoff
def handle_rate_limit(response):
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        logger.warning(f"Rate limited. Waiting {retry_after} seconds")
        time.sleep(retry_after)
        return True
    return False
```

**2. Network Timeouts**
```python
# Configure timeouts and retries
session = requests.Session()
adapter = HTTPAdapter(max_retries=Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504]
))
session.mount('http://', adapter)
session.mount('https://', adapter)
```

**3. Kafka Connection Issues**
```python
# Implement Kafka producer with retry logic
def create_producer_with_retry():
    for attempt in range(3):
        try:
            return KafkaProducer(bootstrap_servers=servers)
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
```

### Recovery Strategies

1. **Graceful Degradation**: Continue with partial data if some sources fail
2. **Circuit Breaker**: Temporarily disable failing extractors
3. **Dead Letter Queue**: Route failed messages for manual review
4. **Checkpointing**: Track last successful extraction for resume capability

## 📊 Data Quality Checks

### Extraction-Time Validations

```python
def validate_flood_data(data):
    """Validate flood warning data"""
    required_fields = ['@id', 'severity', 'floodArea']
    
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    # Validate severity levels
    valid_severities = ['Severe Flood Warning', 'Flood Warning', 'Flood Alert']
    if data['severity'] not in valid_severities:
        raise ValueError(f"Invalid severity: {data['severity']}")
    
    return True
```

### Data Freshness Monitoring

```python
def check_data_freshness(last_extraction_time):
    """Check if data is fresh enough"""
    current_time = datetime.utcnow()
    age_minutes = (current_time - last_extraction_time).total_seconds() / 60
    
    if age_minutes > 30:  # Alert if data is older than 30 minutes
        logger.warning(f"Data is {age_minutes:.1f} minutes old")
        return False
    
    return True
```

## 🔗 Integration with Transform Phase

The Extract phase outputs data to Kafka topics that are consumed by the Transform phase:

```python
# Transform phase reads from these topics
bronze_topics = [
    'flood-warnings',
    'stations', 
    'flood-areas',
    'water-levels',
    'historical-readings'
]

# Spark Structured Streaming consumption
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", ",".join(bronze_topics)) \
    .load()
```

## 🎯 Performance Optimization

### Tips for Optimal Extraction Performance

1. **Parallel Extraction**: Process multiple stations concurrently
2. **Batch API Calls**: Group related requests when possible
3. **Connection Pooling**: Reuse HTTP connections
4. **Async Processing**: Use async/await for I/O bound operations
5. **Memory Management**: Stream large datasets rather than loading in memory

### Scaling Considerations

- **Horizontal Scaling**: Run multiple extractor instances
- **Load Balancing**: Distribute API calls across extractors
- **Kafka Partitioning**: Increase partitions for higher throughput
- **Resource Monitoring**: Monitor CPU, memory, and network usage

---

This Extract phase provides a robust foundation for ingesting flood monitoring data into your data lakehouse! 🌊📥 