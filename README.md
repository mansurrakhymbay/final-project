# Cats Data Pipeline (Airflow + PostgreSQL + dbt)
End-to-end data pipeline built with Airflow, PostgreSQL, and dbt.
The pipeline loads data from an external REST API and transforms it into an analytics-ready data mart.
---

## Data Source

**TheCatAPI**  
https://thecatapi.com/

Entities:
- Breeds
- Images

Relationship:
- images.breed_id → breeds.id

---

## Architecture

Database schemas:
- raw — raw data loaded from API
- staging — cleaned and standardized data (dbt views)
- mart — analytics-ready data (dbt tables)

Main components:
- PostgreSQL — data warehouse
- Airflow — orchestration
- dbt — transformations and tests

---

## Airflow DAGs

### 1. cats_ingestion_dag
- Runs daily
- Loads data from TheCatAPI
- Writes data into:
  - raw.breeds
  - raw.images
- Uses idempotent loading (upsert)

### 2. cats_transformation_dag
- Runs daily
- Executes:
  - dbt run
  - dbt test

---

## dbt Models

### Staging
- staging.stg_breeds (view)
- staging.stg_images (view)

### Mart
- mart.mart_breed_images (table)

---

## dbt Tests

Implemented tests:
- not_null
- unique
- relationships

All tests pass successfully.

---

## How to Run

### 1. Create environment file

Create `.env` file in project root:

```env
CAT_API_KEY=PASTE_YOUR_KEY_HERE
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
