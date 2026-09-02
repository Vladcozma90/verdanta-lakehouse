# Data Dictionary

**Verdanta Group N.V.** — a fictional pan-European garden & outdoor-living
retailer, HQ in Amsterdam, operating across NL, DE, PL, RO, and ES. Five
countries, three currencies (EUR, PLN, RON), five VAT regimes.

Source data is synthetic and deterministic (`SEED = 42`) —
`data/generate_source_data.py` reproduces an identical dataset on every run.

| Property | Value |
|---|---|
| History window | 2024-08-01 → 2026-08-01 |
| Orders / order lines | ~55,000  /  ~130,000 |
| Customers / products / employees / stores / suppliers | 12,035 / 1,500 / 520 / 48 / 40 |

---

## Landing layout

```
landing/<source_system>/<entity>/ingest_date=YYYY-MM-DD/<entity>_YYYYMMDD.<ext>
```

| Source system | Entity | Format | Cadence | Bronze table |
|---|---|---|---|---|
| `erp` | orders | CSV | daily | `erp_orders` |
| `erp` | order_items | Parquet | daily | `erp_order_items` |
| `erp` | returns | CSV | daily | `erp_returns` |
| `crm` | customers | CSV | daily, full snapshot | `crm_customers` |
| `hr` | employees | CSV | weekly, full snapshot | `hr_employees` |
| `pim` | products | JSONL, nested | daily, full snapshot | `pim_products` |
| `pim` | suppliers | CSV | weekly, full snapshot | `pim_suppliers` |
| `reference` | stores | CSV | on change | `reference_stores` |

## Bronze metadata columns

Every bronze table carries these in addition to its source columns, all
STRING except `_ingest_ts`:

- `run_id` — UUID of the ingestion run that wrote the row
- `_file_name` — source file path (`_metadata.file_path`)
- `_source_system` — `erp` / `crm` / `hr` / `pim` / `reference`
- `_ingest_ts` — `TIMESTAMP`, when the row was ingested

Every source column lands as STRING (`cloudFiles.inferColumnTypes=false`) —
typing happens in silver staging, never at ingestion.

## Seeds

- `seed_countries` — `country_code`, `country_name`, `region`, `currency_code`
- `seed_vat_rates` — `country_code`, `vat_class`, `vat_rate`, `valid_from`,
  `valid_to`. Effective-dated: Romania's standard rate moves 19% → 21% on
  2025-08-01, inside the history window.
- `seed_fx_rates_eur` — daily `EUR → {EUR, PLN, RON}` rates

---

## Deliberate data-quality defects

Planted so the test suite has something real to catch. Not cleaned out of
the source on purpose.

| # | Where | Defect | Test / severity |
|---|---|---|---|
| 1 | `customers.email` | ~2% null | `not_null`, `warn` |
| 2 | `customers.marketing_opt_in` | six boolean encodings | normalized in staging |
| 3 | `customers.country_code` | mixed case | `upper()` in staging |
| 4 | `customers` | 35 duplicate people under new IDs | known limitation, not caught |
| 5 | `orders` | ~55 duplicate headers | deduplicated in staging |
| 6 | `orders.customer_id` | 40 IDs absent from customers | `relationships`, `warn` |
| 7 | `orders.order_net_amount` | header ≠ sum of lines, ~174 orders | `assert_order_header_reconciles_to_lines`, `warn` |
| 8 | `order_items.quantity` | zero, 25 rows | `dbt_utils.accepted_range` |
| 9 | `returns.return_ts` | non-ISO (`DD/MM/YYYY HH:MM`) | explicit `to_timestamp()` parse in staging |
| 10 | `employees.last_name` | trailing whitespace | `trim()` in staging |
| 11 | `employees` | terminated staff still store-assigned | expected, not an error |

---

## SCD2 dimensions

| Dim | Strategy | Change tracked |
|---|---|---|
| `dim_customer` | SCD2 (`timestamp`, via `snap_crm_customers`) | loyalty tier, city, country |
| `dim_product` | SCD2 (`timestamp`, via `snap_pim_products`) | list price, category, status |
| `dim_store` | full rebuild | — |
| `dim_employee` | full rebuild | — |
| `dim_supplier` | full rebuild | — |
| `dim_country` | full rebuild, from `seed_countries` | — |
| `dim_date` | generated, static | — |

Every dimension carries an explicit unknown member (`-1` / `XX`) — every
fact foreign key resolves to a real row.

## Star schema

**`fct_order_item`** — one row per order line. **`fct_return`** — one row
per returned line. Both `materialized='incremental', incremental_strategy='merge'`.

`order_id`, `order_number`, `line_number` are degenerate dimensions, carried
directly on the fact rows.