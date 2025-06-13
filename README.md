# Data Lakehouse Project

A comprehensive data lakehouse solution for flood monitoring and water level analytics, built with modern big data technologies including Spark, Delta Lake, Kafka, and MinIO.

## 🏗️ Architecture Overview

This data lakehouse implements a medallion architecture (Bronze → Silver → Gold) for processing flood monitoring data with real-time streaming capabilities and machine learning support.

### Core Components

- **Data Storage**: MinIO (S3-compatible object storage)
- **Data Processing**: Apache Spark with Delta Lake
- **Streaming**: Apache Kafka + Zookeeper
- **SQL Engine**: Trino (distributed SQL query engine)
- **Metadata**: Hive Metastore with MariaDB
- **Visualization**: Apache Superset
- **Development**: Jupyter Lab with Spark integration
- **Orchestration**: Docker Compose

## 📊 Data Pipeline

```
Data Sources → Kafka → Bronze Layer → Silver Layer → Gold Layer → ML/Analytics
                        (Raw Data)    (Cleaned)     (Aggregated)
```



## 📚 Technologies Used

| Technology | Purpose | Version |
|------------|---------|---------|
| Apache Spark | Data Processing | 3.3.2 |
| Delta Lake | Storage Format | Latest |
| Apache Kafka | Streaming | 7.3.0 |
| MinIO | Object Storage | RELEASE.2024-05-10T01-41-38Z |
| Trino | SQL Engine | 414 |
| Apache Superset | Visualization | Latest |
| Hive Metastore | Metadata | 3.0.0 |
| Jupyter Lab | Development | Latest |
| MariaDB | Metadata Storage | 10.5.8 |

