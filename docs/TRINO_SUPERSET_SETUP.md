# Trino + Superset Setup Guide for Gold Layer Analytics

This guide shows you how to set up Trino to query your Delta Lake tables and configure Superset for visualization.

## 🔧 Trino Configuration for Delta Lake

### 1. Configure Trino Delta Catalog

Create or update your Trino configuration to connect to MinIO and read Delta tables:

**File: `docker-image/trino/catalog/delta.properties`**
```properties
connector.name=delta_lake
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.s3.aws-access-key=minio_access_key
hive.s3.aws-secret-key=minio_secret_key
delta.enable-non-concurrent-writes=true
```

### 2. Verify Trino Connection

After starting your services, test Trino connectivity:

```bash
# Connect to Trino CLI
docker exec -it trino trino --server localhost:8080

# List available catalogs
SHOW CATALOGS;

# List schemas (databases) in delta catalog
SHOW SCHEMAS FROM delta;

# List tables in your gold database
SHOW TABLES FROM delta.flood_analytics_gold;
```

### 3. Sample Trino Queries for Gold Layer

```sql
-- View all dimension and fact tables
SHOW TABLES FROM delta.flood_analytics_gold;

-- Query station performance
SELECT 
    station_name,
    river_name,
    region,
    flood_risk_category,
    total_readings,
    avg_water_level,
    high_risk_events
FROM delta.flood_analytics_gold.v_station_performance
ORDER BY high_risk_events DESC
LIMIT 10;

-- Daily trends analysis
SELECT 
    full_date,
    season,
    region,
    SUM(reading_count) as total_readings,
    AVG(avg_water_level) as avg_level,
    SUM(high_risk_readings) as risk_events
FROM delta.flood_analytics_gold.v_daily_trends
WHERE year = 2025
GROUP BY full_date, season, region
ORDER BY full_date;

-- Regional flood risk summary
SELECT 
    region,
    station_count,
    avg_water_level,
    max_water_level,
    total_high_risk_events,
    flood_event_count
FROM delta.flood_analytics_gold.v_regional_analysis
ORDER BY total_high_risk_events DESC;

-- Flood event analysis
SELECT 
    station_key,
    event_severity,
    COUNT(*) as event_count,
    AVG(event_duration_hours) as avg_duration,
    AVG(peak_water_level) as avg_peak_level
FROM delta.flood_analytics_gold.fact_flood_events
GROUP BY station_key, event_severity
ORDER BY event_count DESC;
```

## 📊 Superset Configuration

### 1. Access Superset

1. Navigate to http://localhost:8088
2. Login with: **admin/admin**

### 2. Add Trino Database Connection

1. Go to **Settings** → **Database Connections**
2. Click **+ Database**
3. Select **Trino** as the database type
4. Configure connection:

```
Host: trino
Port: 8080
Database: delta
Username: admin
Password: (leave empty)
```

**SQLAlchemy URI:**
```
trino://admin@trino:8080/delta
```

### 3. Test Connection

Click **Test Connection** to verify Trino connectivity.

### 4. Add Gold Layer Tables as Datasets

1. Go to **Data** → **Datasets**
2. Click **+ Dataset**
3. Select your Trino database
4. Choose schema: `flood_analytics_gold`
5. Add these key tables/views:
   - `v_station_performance`
   - `v_daily_trends`
   - `v_regional_analysis`
   - `fact_water_levels`
   - `fact_daily_summary`
   - `fact_flood_events`

## 📈 Sample Superset Dashboards

### Dashboard 1: Flood Monitoring Overview

**Charts to create:**

1. **Big Number Chart** - Total Stations
   ```sql
   SELECT COUNT(DISTINCT station_key) as total_stations
   FROM flood_analytics_gold.dim_stations
   ```

2. **Line Chart** - Daily Water Levels Trend
   ```sql
   SELECT 
       full_date,
       AVG(avg_water_level) as avg_level
   FROM flood_analytics_gold.v_daily_trends
   WHERE year = 2025
   GROUP BY full_date
   ORDER BY full_date
   ```

3. **Bar Chart** - High Risk Events by Region
   ```sql
   SELECT 
       region,
       SUM(total_high_risk_events) as risk_events
   FROM flood_analytics_gold.v_regional_analysis
   GROUP BY region
   ORDER BY risk_events DESC
   ```

4. **Map Chart** - Station Locations
   ```sql
   SELECT 
       station_name,
       latitude,
       longitude,
       flood_risk_category,
       high_risk_events
   FROM flood_analytics_gold.v_station_performance
   ```

### Dashboard 2: Station Performance Analysis

1. **Table Chart** - Top Risk Stations
   ```sql
   SELECT 
       station_name,
       river_name,
       town,
       flood_risk_category,
       high_risk_events,
       avg_risk_score
   FROM flood_analytics_gold.v_station_performance
   ORDER BY high_risk_events DESC
   LIMIT 20
   ```

2. **Heatmap** - Risk by Region and Season
   ```sql
   SELECT 
       region,
       season,
       SUM(high_risk_readings) as risk_events
   FROM flood_analytics_gold.v_daily_trends
   GROUP BY region, season
   ```

### Dashboard 3: Flood Event Analysis

1. **Histogram** - Event Duration Distribution
   ```sql
   SELECT 
       event_duration_hours,
       COUNT(*) as frequency
   FROM flood_analytics_gold.fact_flood_events
   GROUP BY event_duration_hours
   ```

2. **Pie Chart** - Event Severity Distribution
   ```sql
   SELECT 
       event_severity,
       COUNT(*) as event_count
   FROM flood_analytics_gold.fact_flood_events
   GROUP BY event_severity
   ```

## 🎯 Performance Optimization Tips

### 1. Trino Query Optimization

- Use `fact_daily_summary` for dashboard queries instead of `fact_water_levels` when possible
- Leverage partitioning (year, month) in WHERE clauses
- Use analytical views for complex joins

### 2. Superset Caching

1. Go to **Settings** → **Database** → **Edit** your Trino connection
2. Enable **Cache Timeout**: 3600 seconds (1 hour)
3. For real-time dashboards, set lower cache timeout

### 3. Dashboard Filters

Add these common filters to your dashboards:
- **Date Range Filter** on `full_date`
- **Region Filter** on `region`
- **Station Type Filter** on `station_type`
- **Risk Level Filter** on `flood_risk_category`

## 🚨 Troubleshooting

### Common Issues:

1. **Trino can't find tables**
   - Check Hive Metastore connection
   - Verify Delta Lake catalog configuration
   - Ensure tables are registered in metastore

2. **Superset connection failed**
   - Check Trino service is running
   - Verify network connectivity between containers
   - Check SQLAlchemy URI format

3. **Slow dashboard performance**
   - Use pre-aggregated tables (`fact_daily_summary`)
   - Add appropriate filters
   - Consider enabling Superset caching

### Debug Commands:

```bash
# Check Trino logs
docker logs trino

# Check Hive Metastore connectivity
docker exec trino curl -v hive-metastore:9083

# Test MinIO connectivity from Trino
docker exec trino curl -v minio:9000

# Check available tables in Spark
docker exec spark-master spark-sql -e "SHOW TABLES FROM flood_analytics_gold"
```

## 📚 Next Steps

1. **Create Real-time Alerts**: Set up Superset alerts for high-risk events
2. **Advanced Analytics**: Build predictive models using the ML features
3. **Data Quality Monitoring**: Create dashboards to monitor data quality metrics
4. **User Management**: Set up proper user roles and permissions in Superset

## 🔗 Useful Resources

- [Trino Delta Lake Connector Documentation](https://trino.io/docs/current/connector/delta-lake.html)
- [Superset Documentation](https://superset.apache.org/docs/intro)
- [Delta Lake Documentation](https://docs.delta.io/)

---

Your dimensional model is now ready for production analytics and visualization! 🎉 