# Verdanta Lakehouse

An end-to-end data engineering pipeline for **Verdanta Group N.V.**, a
fictional pan-European garden & outdoor-living retailer operating across the
Netherlands, Germany, Poland, Romania, and Spain.

Databricks + PySpark for ingestion, dbt for transformation and testing,
Airflow for orchestration, Databricks Asset Bundles and GitHub Actions for
deployment — a full medallion lakehouse from raw source files to a tested
star schema.

**[Architecture, design decisions, and repository layout →](./ARCHITECTURE.md)**

**[Data dictionary and planted data-quality defects →](./DATA_DICTIONARY.md)**

---

## Stack

| Layer | Technology |
|---|---|
| Storage | Azure Data Lake Storage Gen2, Unity Catalog |
| Ingestion | Databricks Auto Loader (PySpark) |
| Transformation | dbt (dbt-databricks) |
| Orchestration | Apache Airflow, astronomer-cosmos |
| Deployment | Databricks Asset Bundles, GitHub Actions |

---

## Quickstart

```bash
# 1. Generate synthetic source data (deterministic, seed=42)
python data/generate_source_data.py --out ~/verdanta-source-extracts

# 2. Deploy the Databricks Jobs
databricks bundle deploy -t dev

# 3. Run the pipeline
databricks bundle run bootstrap_tables -t dev --params env=dev
databricks bundle run land_to_landing -t dev --params env=dev,ingest_date=2026-07-28
databricks bundle run bronze_ingestion_daily -t dev --params env=dev

# 4. Build and test the warehouse
cd verdanta_project
dbt deps
dbt build --target dev

# 5. Orchestrate on a schedule (optional)
cd ../airflow
docker compose up -d
# → localhost:8080 — set Connection `databricks_default` and
#   Variable `verdanta_env=dev`, then unpause the three DAGs
```

CI runs lint and `dbt build` on every pull request. Merging to `main`
deploys to `dev` automatically; `prod` deploys are manually triggered.

---

## Known gaps

- Unit tests for the PySpark transform functions — planned, not yet written.
- `dbt docs` site — not yet published.