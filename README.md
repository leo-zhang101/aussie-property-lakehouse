# Australia Property Lakehouse Data Platform

This project demonstrates a modern data engineering pipeline analysing Australian property prices and population density.

## Architecture

Raw Data → Bronze → Silver → Gold → PostgreSQL

### Raw
Original CSV datasets containing property prices and population density.

### Bronze
Raw data ingested into the data lake with minimal processing.

### Silver
Data cleaned and merged into a unified dataset.

### Gold
Analytics table generated with calculated metrics.

### PostgreSQL
Final analytical dataset stored in PostgreSQL for querying.

## Tech Stack

Python  
PostgreSQL  
Docker  
Pandas  
SQLAlchemy  

## Project Structure

data/
    raw/
    bronze/
    silver/
    gold/

src/
    etl/
    pipeline/
    transform/

## Run Pipeline
