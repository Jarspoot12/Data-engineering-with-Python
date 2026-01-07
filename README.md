# Data Engineering with Python & SQL

Hands-on data engineering projects built with Python, SQL, and modern tooling. This repository demonstrates how data flows are built progressively, from raw ingestion to structured datasets.

## What you’ll find here
- Data ingestion from files, APIs, and databases
- Data cleaning and validation
- Efficient Python code for data workflows
- ETL and ELT pipelines
- SQL-driven transformations
- Workflow orchestration with Apache Airflow
- Git workflows used in real engineering teams

Each module is extended with local scripts and exercises that reflect real data engineering scenarios.

## Tech Stack
- Python 3.11+
- pandas, requests, sqlalchemy, psycopg2
- PostgreSQL
- Apache Airflow
- Git & GitHub

## Project Structure
- Each `0X_...` directory represents a focused data engineering domain
- `exercises/`: isolated concepts implemented in code
- `projects/`: end-to-end workflows (ingest → transform → load)
- `data/`: raw, intermediate, and processed datasets
- `sql/`: schemas and analytical queries

## Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
