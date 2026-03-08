# Aussie FinTech Streaming Pipeline

This project is a real-time data engineering pipeline built with Apache Kafka, Apache Spark Structured Streaming, Docker, and PostgreSQL.

It simulates a production-style transaction monitoring workflow that generates, processes, validates, and stores more than 10,000 payment events for downstream analytics.

## Project Goal

The objective of this project is to build an advanced streaming data platform that demonstrates:

- real-time transaction event generation
- Kafka-based event ingestion
- Spark Structured Streaming transformation
- data quality validation
- PostgreSQL warehouse loading
- SQL-based analytics on streaming data

## Target Scale

- 10,000+ streaming events
- multi-attribute transaction records
- real-time ingestion and processing
- persistent storage for SQL analytics

## Planned Architecture

```text
Transaction Generator
      ↓
Kafka Producer
      ↓
Kafka Topic
      ↓
Spark Structured Streaming
      ↓
Validation / Fraud Rules
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

## Example Event Model

```json
{
  "event_id": "uuid",
  "event_ts": "2026-03-08T05:12:11Z",
  "transaction_id": "TXN123456",
  "customer_id": 102938,
  "merchant_id": 48392,
  "merchant_category": "groceries",
  "payment_method": "card",
  "currency": "AUD",
  "amount": 128.45,
  "state": "VIC",
  "channel": "mobile_app",
  "device_type": "ios",
  "transaction_status": "approved",
  "is_international": false,
  "risk_score": 0.13
}
```

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
- implement a Kafka producer for real-time transaction events
