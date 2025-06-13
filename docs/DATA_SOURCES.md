# Data Sources Documentation

## Data Pipeline Overview
This project extracts flood monitoring data from UK Environment Agency APIs and processes it through a medallion architecture (Bronze → Silver → Gold).

## Data Sources

### 1. UK Environment Agency Flood Monitoring API
- **Base URL:** `https://environment.data.gov.uk/flood-monitoring`
- **Documentation:** https://environment.data.gov.uk/flood-monitoring/doc/reference
- **Rate Limits:** No explicit limits mentioned
- **Update Frequency:** Real-time for warnings, hourly for readings

### 2. Data Topics
- `flood-warnings`: Active flood warnings and alerts
- `flood-areas`: Geographic flood risk areas
- `water-levels`: Current water level readings
- `stations`: Monitoring station metadata
- `historical-readings`: 30-day historical water level data

## Data Storage Structure

### Bronze Layer (Raw Data)
```
s3a://data-lakehouse/bronze/
├── flood-warnings/
├── flood-areas/
├── water-levels/
├── stations/
└── historical-readings/
```

### Silver Layer (Cleaned Data)
```
s3a://data-lakehouse/silver/
├── flood_events/
├── station_readings/
└── risk_assessments/
```

### Gold Layer (Analytics-Ready)
```
s3a://data-lakehouse/gold/
├── flood_predictions/
├── risk_metrics/
└── station_summaries/
```

## Data Schemas

### Kafka Message Structure
```json
{
  "kafka_topic": "string",
  "kafka_partition": "int",
  "kafka_offset": "long",
  "kafka_timestamp": "timestamp",
  "json_data": "string",
  "processing_timestamp": "timestamp",
  "year": "int",
  "month": "int", 
  "day": "int"
}
```

### Sample API Response (Stations)
```json
{
  "@context": "http://environment.data.gov.uk/flood-monitoring/meta/context.jsonld",
  "meta": {
    "publisher": "Environment Agency",
    "licence": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "documentation": "http://environment.data.gov.uk/flood-monitoring/doc/reference"
  },
  "items": [
    {
      "@id": "http://environment.data.gov.uk/flood-monitoring/id/stations/1029TH",
      "RLOIid": "7041",
      "catchmentName": "Colne",
      "dateOpened": "1992-01-01",
      "easting": 505055,
      "label": "Colne at Lexden",
      "lat": 51.8867,
      "long": 0.9031,
      "measures": [...],
      "northing": 225610,
      "notation": "1029TH",
      "riverName": "Colne",
      "stageScale": "http://environment.data.gov.uk/flood-monitoring/id/stations/1029TH/stageScale",
      "stationReference": "1029TH",
      "status": "http://environment.data.gov.uk/flood-monitoring/def/core/statusActive",
      "town": "Colchester",
      "type": ["http://environment.data.gov.uk/flood-monitoring/def/core/Station"],
      "wiskiID": "1029TH"
    }
  ]
}
```

## Environment Variables Required
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
```

## Data Retention Policy
- **Bronze:** Keep all raw data (permanent)
- **Silver:** Keep cleaned data for 2 years
- **Gold:** Keep aggregated data for 5 years
- **Checkpoints:** Clean up after successful processing

## Data Quality Checks
- Validate JSON structure on ingestion
- Check for duplicate records by timestamp + station ID
- Monitor data freshness (alerts if no data > 1 hour)
- Validate coordinate bounds for station locations
