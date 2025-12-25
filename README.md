E-Commerce Data Pipeline Project

Student Name: Rowtu Divya Sri Lakshmi
Roll Number: 23MH1A05O5
Submission Date: [Insert Date]

Prerequisites

Python 3.8+

PostgreSQL 12+

Docker & Docker Compose

Git

Tableau Public OR Power BI Desktop

Installation & Setup

Clone the repository

git clone https://github.com/Divyasrirowtu/ecommerce-data-pipeline-23MH1A05O5.git
cd ecommerce-data-pipeline-23MH1A05O5


Create and activate Python virtual environment

python -m venv venv
.\venv\Scripts\Activate.ps1   # For PowerShell
pip install -r requirements.txt


Setup PostgreSQL (Docker recommended)

docker pull postgres:15
docker run --name postgres-db -e POSTGRES_PASSWORD=postgres123 -e POSTGRES_DB=ecommerce -p 5432:5432 -d postgres:15
docker ps  # Verify container is running

Phase 2: Data Generation & Ingestion
2.1 Generate Data
python scripts/data_generation/generate_data.py


Generates realistic CSV files for: customers, products, orders, payments

Stored in: data/raw/

2.2 Ingest Data into Staging
python scripts/ingestion/ingest_to_staging.py


Loads CSV files into staging tables in PostgreSQL

Updates: data/staging/ingestion_summary.json

2.3 Transform & Load to Warehouse
python scripts/transformation/load_to_warehouse.py


Cleans, deduplicates, and transforms data from staging

Loads into final warehouse tables for analytics

Phase 3: Analytics / Reporting

Query final tables for insights

Example SQL:

SELECT c.customer_id, c.first_name, COUNT(o.order_id) AS total_orders
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name;


Use Tableau Public or Power BI to visualize final tables.

Database Details

Host: localhost

Port: 5432

Database: ecommerce

User: postgres

Password: postgres123

Folder Structure
ecommerce-data-pipeline-23MH1A05O5/
│
├─ data/
│   ├─ raw/               # Generated CSVs
│   └─ staging/           # Ingestion summary
│
├─ scripts/
│   ├─ data_generation/   # Generate data scripts
│   ├─ ingestion/         # Load to staging scripts
│   └─ transformation/    # Load from staging to warehouse
│
├─ venv/                  # Python virtual environment
├─ requirements.txt
└─ README.

## Project Architecture
Raw Data → Staging → Production → Warehouse → Analytics → BI Dashboard

## Technology Stack
- Python (Faker, Pandas)
- PostgreSQL
- Docker
- Pytest
- Tableau / Power BI

## Project Structure
ecommerce-data-pipeline/
├── scripts/
├── sql/
├── tests/
├── dashboards/
├── docs/
