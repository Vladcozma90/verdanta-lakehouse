# Architecture

## Data flow

```
source systems (ERP / CRM / PIM, simulated)
        │
        │  Databricks Job: land_to_landing (Python)
        ▼
   landing/            raw files, immutable, partitioned by ingest_date
        │
        │  Databricks Jobs: bronze_ingestion_* (PySpark + Auto Loader)
        ▼
   bronze/             1:1 with source, all-STRING, append-only Delta
        │
        │  dbt: staging (cast/clean) → snapshots (SCD2) → intermediate (joins)
        ▼
   silver/
        │
        │  dbt: marts — surrogate keys, unknown members, star schema
        ▼
   gold/               dim_customer, dim_product, dim_store, dim_employee,
                        dim_supplier, dim_country, dim_date,
                        fct_order_item, fct_return
```

`dev` uses Databricks-managed Unity Catalog volumes for landing and
checkpoints. `prod` uses external volumes backed by Azure Data Lake Storage
Gen2 — the only difference between environments is where the bytes live;
code and pipeline logic are identical.

## Orchestration

Three Airflow DAGs, matching each source's real delivery cadence rather than
running everything on one schedule:

| DAG | Schedule | Covers |
|---|---|---|
| `verdanta_daily` | `0 4 * * *` | orders, order_items, returns, customers, products + downstream dbt |
| `verdanta_weekly` | Mondays | employees, suppliers + downstream dbt |
| `verdanta_stores` | on-demand | stores — changes only when a location opens or closes |

Each Databricks Job is defined declaratively in `databricks.yml` (Databricks
Asset Bundles) and deployed via GitHub Actions on merge to `main`.

## Grain statements

> **`fct_order_item`** — one row per line item on a customer order.

> **`fct_return`** — one row per returned line item on a customer order.

`order_id`, `order_number`, and `line_number` are degenerate dimensions,
carried directly on the fact rows rather than modeled separately, since they
describe the transaction itself rather than a reusable business entity.

## Design decisions

- **Airflow over ADF.** Open source, code-as-config, unit-testable DAGs,
  portable off Azure.
- **`timestamp` snapshot strategy over `check`.** `check` sets
  `dbt_valid_from` to when the job ran, not when the underlying fact
  changed — every source table carries `updated_at` for this reason.
- **SCD2 on `dim_customer` and `dim_product` only.** These are the
  attributes a report needs "as of the transaction date" — loyalty tier,
  category, price. Store, employee, and supplier are full-rebuild; there's
  no analytical value in tracking their history at this scale.
- **`warn` vs `error` test severity is chosen per test, not defaulted.**
  Known, expected data shapes (orphan customer IDs, null store IDs on
  online orders) warn. An order whose header doesn't reconcile to its
  lines errors — see the defects table.
- **Hash surrogate keys with an explicit unknown member on every
  dimension.** Every fact FK resolves to a real row, so `not_null` — a
  stronger, cheaper test than `relationships` — is sufficient everywhere.
- **Databricks Asset Bundles, not UI-managed jobs.** Job definitions are
  versioned text, reviewable in a PR diff, reproducible in a new workspace
  with one `bundle deploy`.
- **CI validates, CD deploys.** Every PR runs lint and `dbt build` against
  a live `dev` schema. Merging to `main` deploys to `dev` automatically;
  `prod` deploys are gated behind a manual trigger.
- **Landing lands in mixed formats.** CSV, Parquet, and nested JSONL,
  matching what each source system actually produces — proving struct
  flattening and array-to-bridge-table handling, not just one ingestion
  pattern repeated eight times.

## Data-quality defects

The source data carries deliberately planted defects — duplicate order
headers, inconsistent boolean encodings, non-ISO timestamps, orphan foreign
keys, and a reconciliation mismatch on a subset of orders. Each is caught by
a named dbt test with an intentional severity.

Full list: **[`DATA_DICTIONARY.md`](./DATA_DICTIONARY.md)**

## Repository layout

```
verdanta-lakehouse/
├── README.md
├── ARCHITECTURE.md
├── DATA_DICTIONARY.md
├── PROJECT_PLAN.md
├── pyproject.toml
├── databricks.yml            Asset Bundle: all Databricks Jobs
├── data/                   generate_source_data.py
├── jobs/                      job entry points
│   ├── land/                    run_land.py
│   ├── bootstrap/                run_bootstrap.py
│   └── bronze/                    run_bronze_customers.py, _orders.py, ...
├── src/verdanta/
│   ├── common/                  config, paths, logging
│   ├── extract/                   land_partition(), ADLS upload
│   └── pipelines/
│       ├── bronze/                  Auto Loader ingestion, bootstrap DDL
│       └── bootstrap/                per-entity table bootstrap
├── verdanta_project/          dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/                    custom schema
│   ├── seeds/                    countries, VAT rates, FX rates
│   ├── models/
│   │   ├── source/                 source bronze tables
│   │   ├── staging/                 1:1 with bronze, cast + clean
│   │   ├── intermediate/            FX conversion, enrichment, bridge tables
│   │   └── marts/                    dim_*, fct_*
│   ├── snapshots/                 SCD2 (customer, product)
│   └── tests/                      singular tests
├── airflow/
│   ├── dags/                      verdanta_daily / _weekly / _stores
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .env                       gitignored
├── conf/                      dev.yml / prod.yml — the only environment diff
├── tests/                     PySpark unit tests (planned)
└── .github/workflows/         ci.yml, cd.yml