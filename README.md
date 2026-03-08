# Kafka Streaming Data Pipeline

This project is a real-time data engineering pipeline built with Apache Kafka, Apache Spark Structured Streaming, Docker, and PostgreSQL.

It is designed to simulate a production-style streaming workflow that generates, processes, validates, and stores more than 10,000 events for downstream analytics.

## Project Goal

The objective of this project is to build an advanced streaming data platform that demonstrates:

- real-time event generation
- Kafka-based event ingestion
- Spark Structured Streaming transformation
- data quality validation
- PostgreSQL warehouse loading
- SQL-based analytics on streaming data

## Target Scale

- 10,000+ streaming events
- multiple event attributes
- real-time ingestion and processing pipeline
- persistent storage for downstream SQL analysis

## Planned Architecture

```text
Event Generator
      ↓
Kafka Producer
      ↓
Kafka Topic
      ↓
Spark Structured Streaming
      ↓
Validation / Cleaning
      ↓
PostgreSQL Warehouse
      ↓
SQL Analytics
```

## Tech Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- Docker
- PostgreSQL
- SQLAlchemy

## Project Structure

```text
kafka-streaming-pipeline/
├── README.md
├── requirements.txt
├── docker/
│   └── docker-compose.yml
├── src/
│   ├── producer/
│   ├── streaming/
│   ├── warehouse/
│   ├── quality/
│   └── utils/
├── data/
│   ├── raw/
│   └── checkpoints/
├── docs/
│   └── architecture/
└── sql/
    └── init.sql
```

## Status

Project setup in progress.

Next step:
- build the local Kafka + Spark + PostgreSQL Docker environment
