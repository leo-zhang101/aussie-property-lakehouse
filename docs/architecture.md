Australia Property Lakehouse Data Platform

Architecture

Raw Data (CSV)
      │
      ▼
Bronze Layer
Minimal transformation
Data copied from raw zone
      │
      ▼
Silver Layer
Data cleaning
Standardisation
Dataset merging
      │
      ▼
Gold Layer
Analytics dataset
Price per density calculation
      │
      ▼
PostgreSQL
Analytics storage
Queryable tables
