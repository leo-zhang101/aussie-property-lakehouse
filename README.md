# Real-Time FinTech Fraud Detection Pipeline

This project simulates a real-time financial transaction system and demonstrates how a modern data platform processes and analyzes streaming payment events.

Transactions are generated continuously, streamed through Apache Kafka, processed using Spark Structured Streaming, and stored in PostgreSQL for downstream analytics and fraud monitoring.

The platform also includes Airflow orchestration, dbt data modeling, and a fraud analytics layer.

---

# System Overview

This pipeline represents a simplified version of how fintech platforms monitor transaction risk in real time.

Key components:

- streaming event ingestion
- real-time data processing
- fraud detection rules
- analytical data warehouse
- pipeline orchestration

---

# Architecture

Transaction Generator  
↓  
Kafka Producer  
↓  
Kafka Topic (payment_events)  
↓  
Spark Structured Streaming  
↓  
Fraud Detection Rules  
↓  
PostgreSQL Warehouse  
↓  
dbt Data Models  
↓  
Fraud Analytics Queries  

Airflow orchestrates the Kafka producer, streaming job, and dbt transformations.

---

# Technology Stack

| Layer | Technology |
|------|-------------|
Streaming | Apache Kafka |
Processing | Spark Structured Streaming |
Programming | Python |
Data Warehouse | PostgreSQL |
Data Modeling | dbt |
Orchestration | Apache Airflow |
Containerization | Docker |
Analytics | SQL |

---

# Pipeline Flow

1. A Python transaction generator simulates payment events.

2. Events are sent to Kafka via a producer.

3. Spark Structured Streaming consumes the Kafka topic.

4. Fraud rules classify transactions in real time.

5. Cleaned transactions are written to PostgreSQL.

6. dbt models build warehouse tables.

7. SQL queries power fraud monitoring and reporting.

8. Airflow orchestrates scheduled workflows.

---

# Data Model

The warehouse uses a simplified analytical structure.

### Fact Table

fact_transactions

| column | description |
|------|------|
transaction_id | unique transaction id |
event_ts | event timestamp |
customer_id | customer identifier |
merchant_id | merchant identifier |
amount | payment amount |
currency | transaction currency |
state | Australian state |
risk_score | fraud risk score |
fraud_flag | fraud classification |

---

# Fraud Detection Rules

Fraud classification is implemented inside the Spark streaming job.

HIGH_RISK → risk_score > 0.8  
HIGH_AMOUNT → amount > 4000 AUD  
INTERNATIONAL → is_international = true  
NORMAL → otherwise  

These rules simulate a simplified fraud detection engine used in payment platforms.

---

# Example Transaction Event

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

---

# Example Analytics Queries

### Fraud Rate by State

```sql
SELECT
    state,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN fraud_flag <> 'NORMAL' THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(
        SUM(CASE WHEN fraud_flag <> 'NORMAL' THEN 1 ELSE 0 END)::numeric / COUNT(*),
        4
    ) AS fraud_rate
FROM fact_transactions
GROUP BY state
ORDER BY fraud_rate DESC;
```

### International Fraud Comparison

```sql
SELECT
    is_international,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN fraud_flag <> 'NORMAL' THEN 1 ELSE 0 END) AS fraud_transactions
FROM fact_transactions
GROUP BY is_international;
```

---

# Pipeline Performance

This pipeline processes simulated financial transactions in real time.

Processing metrics:

- events processed: 200,000+
- streaming engine: Spark Structured Streaming
- message broker: Kafka
- storage layer: PostgreSQL

System capabilities:

- continuous streaming ingestion
- rule-based fraud classification
- structured warehouse storage
- analytical fraud monitoring

---

# Project Structure

```
real-time-fintech-fraud-pipeline

├── README.md
├── requirements.txt
│
├── docker
│   └── docker-compose.yml
│
├── airflow
│   └── dags
│       └── fraud_pipeline_dag.py
│
├── dbt
│   └── models
│       └── fact_transactions.sql
│
├── src
│   ├── producer
│   │   └── event_producer.py
│   │
│   ├── streaming
│   │   └── spark_streaming_consumer.py
│   │
│   ├── warehouse
│   └── utils
│
├── sql
│   ├── create_tables.sql
│   └── analytics_queries.sql
│
└── data
    ├── raw
    └── checkpoints
```

---

# Running the Pipeline

### Clone repository

```
git clone https://github.com/yourusername/real-time-fintech-fraud-pipeline.git
cd real-time-fintech-fraud-pipeline
```

### Start infrastructure

```
docker compose up -d
```

This launches:

- Kafka
- Zookeeper
- PostgreSQL
- Airflow

### Run transaction generator

```
python src/producer/event_producer.py
```

### Start streaming job

```
spark-submit src/streaming/spark_streaming_consumer.py
```

### Run dbt models

```
dbt run
```

### Airflow UI

```
http://localhost:8080
```

---

# Engineering Highlights

- built an end-to-end real-time streaming data pipeline
- processed over 200k simulated financial transactions
- implemented Kafka event ingestion
- implemented Spark Structured Streaming processing
- designed a PostgreSQL analytical warehouse
- implemented dbt transformations
- orchestrated workflows with Airflow
- containerized the platform using Docker

---

# Future Improvements

Possible extensions:

- machine learning fraud models
- real-time monitoring dashboards
- cloud deployment (AWS / GCP)
- streaming feature store

---

# Resume Bullet Points

Example resume descriptions for this project:

• Designed and implemented a real-time financial transaction processing pipeline using Apache Kafka and Spark Structured Streaming.

• Built a streaming data platform capable of processing over 200,000 simulated payment events.

• Implemented rule-based fraud detection logic to classify high-risk, high-amount, and international transactions in real time.

• Developed a PostgreSQL data warehouse to support analytical fraud monitoring queries.

• Implemented dbt data models to structure warehouse tables and enable reproducible data transformations.

• Orchestrated data workflows using Apache Airflow to automate streaming and transformation tasks.

• Containerized the entire platform using Docker to enable reproducible local deployment.

• Developed SQL analytics queries to analyze fraud rates, geographic patterns, and transaction risk distribution.

