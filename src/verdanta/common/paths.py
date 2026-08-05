from datetime import date

SOURCES = {
    "erp":       [("orders", "csv"), ("order_items", "parquet"), ("returns", "csv")],
    "crm":       [("customers", "csv")],
    "hr":        [("employees", "csv")],
    "pim":       [("products", "jsonl"), ("suppliers", "csv")],
    "reference": [("stores", "csv")],
}


def landing_dir(root: str, system: str, entity: str, ingest_date: date | None = None) -> str:
    base = f"{root.rstrip('/')}/{system}/{entity}"
    return base if ingest_date is None else f"{base}/ingest_date={ingest_date:%Y-%m-%d}"


def landing_file(root: str, system: str, entity: str, ext: str, ingest_date: date) -> str:
    return f"{landing_dir(root, system, entity, ingest_date)}/{entity}_{ingest_date:%Y%m%d}.{ext}"