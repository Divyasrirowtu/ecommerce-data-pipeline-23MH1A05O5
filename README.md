# E-Commerce Data Pipeline & Analytics Platform

## Project Architecture

```
Raw CSV Data
   │
   ▼
Staging Schema (PostgreSQL)
   │  Data Quality Checks
   ▼
Production Schema (3NF)
   │  Transformations & Enrichment
   ▼
Warehouse Schema (Star Schema)
   │  Aggregations & Analytics
   ▼
Analytics Outputs (CSV)
   │
   ▼
BI Dashboards (Tableau / Power BI)
```

---

## Technology Stack

* **Data Generation:** Python, Faker
* **Database:** PostgreSQL 15
* **ETL / Transformations:** Python (Pandas, SQLAlchemy, psycopg2)
* **Orchestration:** Python Scheduler
* **Monitoring:** Custom Python + SQL
* **BI & Visualization:** Tableau Public / Power BI Desktop
* **Containerization:** Docker & Docker Compose
* **Testing:** Pytest + Coverage
* **Version Control:** Git & GitHub

---

## Project Structure

```
ecommerce-data-pipeline/
├── config/
├── data/
│   ├── raw/
│   ├── staging/
│   ├── processed/
├── dashboards/
│   ├── screenshots/
│   ├── tableau/
│   └── powerbi/
├── docs/
│   ├── architecture.md
│   └── dashboard_guide.md
├── logs/
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── monitoring/
│   ├── pipeline_orchestrator.py
│   ├── scheduler.py
│   └── cleanup_old_data.py
├── sql/
│   └── queries/
├── tests/
│   ├── test_data_generation.py
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   ├── test_quality_checks.py
│   └── pytest.ini
├── run_tests.sh
├── requirements.txt
└── README.md
```

---

## Setup Instructions

1. Clone the repository

```bash
git clone <repo-url>
cd ecommerce-data-pipeline
```

2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

4. Start PostgreSQL using Docker

```bash
docker-compose up -d
```

---

## Running the Pipeline

### Full Pipeline Execution

```bash
python scripts/pipeline_orchestrator.py
```

### Individual Steps

```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

---

## Running Tests

```bash
bash run_tests.sh
```

OR

```bash
pytest tests/ -v
```

---

## Dashboard Access

* **Tableau Public URL:** (Add your published link)
* **Power BI File:** `dashboards/powerbi/ecommerce_analytics.pbix`
* **Screenshots:** `dashboards/screenshots/`

---

## Database Schemas

### Staging Schema

* staging.customers
* staging.products
* staging.transactions
* staging.transaction_items

### Production Schema

* production.customers
* production.products
* production.transactions
* production.transaction_items

### Warehouse Schema

* warehouse.dim_customers
* warehouse.dim_products
* warehouse.dim_date
* warehouse.dim_payment_method
* warehouse.fact_sales
* warehouse.agg_daily_sales
* warehouse.agg_product_performance
* warehouse.agg_customer_metrics

---

## Key Insights from Analytics

* Electronics category generates highest revenue
* Revenue shows steady month-over-month growth
* VIP customers contribute majority of total revenue
* Digital payment methods dominate transactions

---

## Challenges & Solutions

1. **Data quality issues** → Implemented automated quality checks
2. **Duplicate data on reruns** → Ensured idempotent pipeline design
3. **Pipeline failures** → Added retry logic with exponential backoff
4. **Performance issues** → Introduced aggregation tables

---

## Future Enhancements

* Real-time ingestion using Apache Kafka
* Cloud deployment (AWS / GCP / Azure)
* Machine learning models for demand forecasting
* Real-time alerting and notifications

---

## Contact

**Name:** Divya Sri Rowtu
**Roll Number:** 23MH1A05O5
**Email:** 23MH1A05O5@acoe.edu.in
