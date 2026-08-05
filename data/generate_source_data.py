#!/usr/bin/env python3
"""
Verdanta Group - synthetic source-system data generator
=======================================================

Generates a landing-zone-shaped set of extracts for a fictional pan-European
garden & outdoor-living retailer, for use as the source layer of an end-to-end
data engineering portfolio project.

Design notes
------------
* Three "source systems" are simulated, each with its own extract cadence and
  file format, because a real landing zone is never homogeneous:
      erp        -> orders (CSV), order_items (Parquet), returns (CSV)
      crm        -> customers (CSV, full snapshot)
      hr         -> employees (CSV, full snapshot)
      pim        -> products (JSONL, nested), suppliers (CSV)
      reference  -> stores (CSV)
* Everything is deterministic: fix SEED and you regenerate byte-identical files,
  which is what makes CI-runnable pipeline tests possible.
* Deliberate data-quality defects are injected and documented in DATA_DICTIONARY.md.
  They exist so that dbt tests / quarantine logic have something to catch.
* Volumes are configurable at the top. Defaults are sized to be credible but to
  stay comfortably inside a free Databricks / local Spark environment.

Usage
-----
    python generate_source_data.py --out ./landing [--seed 42] [--scale 1.0]
"""

from __future__ import annotations

import argparse
import json
import math
import random
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------

SEED = 42

# Business history window. HISTORY_END is the cut-off of the initial full extract;
# the days after it are emitted as daily incremental extracts.
HISTORY_START = date(2024, 8, 1)
HISTORY_END = date(2026, 7, 27)
INCREMENTAL_DAYS = 5  # 2026-07-28 .. 2026-08-01
EXTRACT_DATE = date(2026, 8, 1)

N_CUSTOMERS = 12_000
N_PRODUCTS = 1_500
N_EMPLOYEES = 520
N_SUPPLIERS = 40
N_ORDERS = 55_000

COMPANY = "Verdanta Group N.V."

# country_code -> (country_name, currency, region, weight in the business)
COUNTRIES = {
    "NL": ("Netherlands", "EUR", "Benelux", 0.26),
    "DE": ("Germany", "EUR", "DACH", 0.31),
    "PL": ("Poland", "PLN", "CEE", 0.17),
    "RO": ("Romania", "RON", "CEE", 0.11),
    "ES": ("Spain", "EUR", "Iberia", 0.15),
}

CITIES = {
    "NL": ["Amsterdam", "Rotterdam", "Utrecht", "Eindhoven", "Groningen", "Breda", "Tilburg", "Nijmegen"],
    "DE": ["Berlin", "Hamburg", "Munchen", "Koln", "Frankfurt am Main", "Stuttgart", "Dusseldorf", "Leipzig", "Dresden", "Hannover"],
    "PL": ["Warszawa", "Krakow", "Wroclaw", "Poznan", "Gdansk", "Lodz", "Katowice"],
    "RO": ["Bucuresti", "Cluj-Napoca", "Timisoara", "Iasi", "Brasov", "Constanta"],
    "ES": ["Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Malaga", "Bilbao"],
}

# Standard/reduced VAT by country, with an effective-dated change in RO so that the
# VAT lookup cannot be a simple static join.
VAT_RATES = [
    # country, vat_class, rate, valid_from, valid_to
    ("NL", "STANDARD", 0.21, "2000-01-01", None),
    ("NL", "REDUCED", 0.09, "2000-01-01", None),
    ("DE", "STANDARD", 0.19, "2000-01-01", None),
    ("DE", "REDUCED", 0.07, "2000-01-01", None),
    ("PL", "STANDARD", 0.23, "2000-01-01", None),
    ("PL", "REDUCED", 0.08, "2000-01-01", None),
    ("ES", "STANDARD", 0.21, "2000-01-01", None),
    ("ES", "REDUCED", 0.10, "2000-01-01", None),
    ("RO", "STANDARD", 0.19, "2000-01-01", "2025-07-31"),
    ("RO", "REDUCED", 0.09, "2000-01-01", "2025-07-31"),
    ("RO", "STANDARD", 0.21, "2025-08-01", None),
    ("RO", "REDUCED", 0.11, "2025-08-01", None),
]

# Garden & outdoor-living taxonomy
CATEGORY_TREE = {
    "Plants & Seeds": ["Shrubs & Hedging", "Perennials", "Bulbs", "Vegetable Seeds", "Indoor Plants"],
    "Garden Tools": ["Hand Tools", "Power Tools", "Cutting & Pruning", "Digging & Soil"],
    "Outdoor Furniture": ["Dining Sets", "Lounge Sets", "Parasols & Shade", "Cushions & Covers"],
    "BBQ & Grilling": ["Charcoal BBQ", "Gas BBQ", "Pellet & Smokers", "BBQ Accessories"],
    "Watering & Irrigation": ["Hoses & Reels", "Sprinklers", "Drip Systems", "Watering Cans"],
    "Pots & Planters": ["Terracotta", "Ceramic", "Fibreglass", "Raised Beds"],
    "Lawn Care": ["Mowers", "Fertiliser", "Grass Seed", "Trimmers"],
    "Outdoor Lighting": ["Solar Lighting", "Festoon & String", "Path & Bollard"],
    "Garden Structures": ["Sheds", "Pergolas", "Greenhouses", "Fencing"],
    "Pest & Weed Control": ["Weed Killer", "Insect Control", "Netting & Barriers"],
}
CATEGORY_L1 = "Garden & Outdoor Living"

OWN_BRANDS = ["VerdaPro", "GreenLine", "Terrafix", "Hofstede"]
THIRD_PARTY_BRANDS = [
    "Kraftgarten", "Wisteria & Co", "Steinbach Werke", "Hollandia Tuin", "Belmonte",
    "Ostrowski Narzedzia", "Solaris Vega", "Nordveld", "Aurelia Living", "Panteleon",
]

# Monthly demand multipliers: garden retail is strongly seasonal.
SEASONALITY = {
    1: 0.42, 2: 0.55, 3: 1.05, 4: 1.62, 5: 1.85, 6: 1.48,
    7: 1.20, 8: 1.05, 9: 0.92, 10: 0.78, 11: 0.68, 12: 0.80,
}

FIRST_NAMES = {
    "NL": ["Daan", "Sanne", "Bram", "Lotte", "Sven", "Fenna", "Thijs", "Anouk", "Ruben", "Marijke", "Joost", "Eva"],
    "DE": ["Lukas", "Hannah", "Jonas", "Lena", "Felix", "Mia", "Tobias", "Greta", "Sebastian", "Katharina", "Niklas", "Johanna"],
    "PL": ["Piotr", "Katarzyna", "Marek", "Agnieszka", "Tomasz", "Zofia", "Jakub", "Magdalena", "Krzysztof", "Ewa", "Michal", "Anna"],
    "RO": ["Andrei", "Ioana", "Mihai", "Elena", "Radu", "Alexandra", "Vlad", "Cristina", "Bogdan", "Ana-Maria", "Stefan", "Diana"],
    "ES": ["Javier", "Lucia", "Alvaro", "Carmen", "Sergio", "Marta", "Pablo", "Elena", "Diego", "Nuria", "Adrian", "Paula"],
}
LAST_NAMES = {
    "NL": ["de Vries", "Jansen", "van Dijk", "Bakker", "Visser", "Smit", "Meijer", "de Boer", "Mulder", "Bos"],
    "DE": ["Muller", "Schmidt", "Schneider", "Fischer", "Weber", "Wagner", "Becker", "Hoffmann", "Schafer", "Koch"],
    "PL": ["Nowak", "Kowalski", "Wisniewski", "Wojcik", "Kowalczyk", "Kaminski", "Lewandowski", "Zielinski", "Szymanski", "Dabrowski"],
    "RO": ["Popescu", "Ionescu", "Popa", "Radu", "Stoica", "Dumitru", "Marin", "Constantin", "Georgescu", "Munteanu"],
    "ES": ["Garcia", "Martinez", "Lopez", "Sanchez", "Gonzalez", "Rodriguez", "Fernandez", "Perez", "Gomez", "Ruiz"],
}

CITY_COORDS = {
    "Amsterdam": (52.3676, 4.9041), "Rotterdam": (51.9244, 4.4777), "Utrecht": (52.0907, 5.1214),
    "Eindhoven": (51.4416, 5.4697), "Groningen": (53.2194, 6.5665), "Breda": (51.5719, 4.7683),
    "Tilburg": (51.5555, 5.0913), "Nijmegen": (51.8126, 5.8372),
    "Berlin": (52.5200, 13.4050), "Hamburg": (53.5511, 9.9937), "Munchen": (48.1351, 11.5820),
    "Koln": (50.9375, 6.9603), "Frankfurt am Main": (50.1109, 8.6821), "Stuttgart": (48.7758, 9.1829),
    "Dusseldorf": (51.2277, 6.7735), "Leipzig": (51.3397, 12.3731), "Dresden": (51.0504, 13.7373),
    "Hannover": (52.3759, 9.7320),
    "Warszawa": (52.2297, 21.0122), "Krakow": (50.0647, 19.9450), "Wroclaw": (51.1079, 17.0385),
    "Poznan": (52.4064, 16.9252), "Gdansk": (54.3520, 18.6466), "Lodz": (51.7592, 19.4560),
    "Katowice": (50.2649, 19.0238),
    "Bucuresti": (44.4268, 26.1025), "Cluj-Napoca": (46.7712, 23.6236), "Timisoara": (45.7489, 21.2087),
    "Iasi": (47.1585, 27.6014), "Brasov": (45.6427, 25.5887), "Constanta": (44.1598, 28.6348),
    "Madrid": (40.4168, -3.7038), "Barcelona": (41.3851, 2.1734), "Valencia": (39.4699, -0.3763),
    "Sevilla": (37.3891, -5.9845), "Zaragoza": (41.6488, -0.8891), "Malaga": (36.7213, -4.4214),
    "Bilbao": (43.2630, -2.9350),
}

STREETS = {
    "NL": ["Kerkstraat", "Hoofdstraat", "Molenpad", "Industrieweg", "Lindenlaan", "Julianastraat"],
    "DE": ["Ringstrasse", "Gartenweg", "Hauptstrasse", "Bahnhofstrasse", "Lindenweg", "Industriestrasse"],
    "PL": ["ul. Ogrodowa", "ul. Kwiatowa", "Aleja Handlowa", "ul. Polna", "ul. Lipowa", "ul. Przemyslowa"],
    "RO": ["Str. Salciilor", "Bulevardul Comercial", "Str. Grivitei", "Calea Victoriei", "Str. Florilor", "Str. Industriei"],
    "ES": ["Calle Jardines", "Avenida del Parque", "Calle Mayor", "Paseo de la Ribera", "Calle Olivos", "Poligono Industrial"],
}

LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
CHANNELS = ["STORE", "ONLINE", "MARKETPLACE"]
PAYMENT_METHODS = ["CARD", "IDEAL", "PAYPAL", "BLIK", "CASH", "BANK_TRANSFER", "GIFT_CARD"]
RETURN_REASONS = ["DAMAGED_IN_TRANSIT", "NOT_AS_DESCRIBED", "CHANGED_MIND", "WRONG_ITEM_SENT", "FAULTY", "LATE_DELIVERY"]
STORE_FORMATS = ["FLAGSHIP", "STANDARD", "COMPACT", "GARDEN_CENTRE"]

rng = np.random.default_rng(SEED)
pyrng = random.Random(SEED)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def slugify(value: str) -> str:
    norm = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return "".join(ch.lower() if ch.isalnum() else "." for ch in norm).strip(".")


def pick(seq, n=None, p=None):
    if n is None:
        return seq[int(rng.choice(len(seq), p=p))]
    idx = rng.choice(len(seq), size=n, p=p)
    return [seq[i] for i in idx]


def rand_dates(start: date, end: date, n: int) -> np.ndarray:
    span = (end - start).days
    return np.array([start + timedelta(days=int(d)) for d in rng.integers(0, span + 1, n)])


def ts(d: date, hour_lo=7, hour_hi=21) -> datetime:
    return datetime(
        d.year, d.month, d.day,
        int(rng.integers(hour_lo, hour_hi)),
        int(rng.integers(0, 60)),
        int(rng.integers(0, 60)),
    )


def iso(dt) -> str | None:
    if dt is None or (isinstance(dt, float) and math.isnan(dt)):
        return None
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d")


def money(x: float) -> float:
    return float(np.round(x, 2))


def postal_code(cc: str) -> str:
    if cc == "NL":
        return f"{int(rng.integers(1000, 9999))} {pick(list('ABCDEFGHJKLMNPRSTVWXZ'))}{pick(list('ABCDEFGHJKLMNPRSTVWXZ'))}"
    if cc == "PL":
        return f"{int(rng.integers(0, 100)):02d}-{int(rng.integers(0, 1000)):03d}"
    if cc == "RO":
        return f"{int(rng.integers(100000, 999999))}"
    return f"{int(rng.integers(1000, 99999)):05d}"


def street(cc: str) -> str:
    return f"{int(rng.integers(1, 240))} {pick(STREETS[cc])}"


def city_coords(city: str) -> tuple[float, float]:
    lat, lon = CITY_COORDS.get(city, (50.0, 10.0))
    return round(lat + float(rng.normal(0, 0.045)), 6), round(lon + float(rng.normal(0, 0.06)), 6)


def country_codes_weighted(n: int) -> list[str]:
    codes = list(COUNTRIES)
    weights = np.array([COUNTRIES[c][3] for c in codes], dtype=float)
    return pick(codes, n=n, p=weights / weights.sum())


def vat_rate_for(country: str, vat_class: str, on: date) -> float:
    for c, cls, rate, vf, vt in VAT_RATES:
        if c != country or cls != vat_class:
            continue
        if date.fromisoformat(vf) <= on and (vt is None or on <= date.fromisoformat(vt)):
            return rate
    raise ValueError(f"no VAT rate for {country}/{vat_class} on {on}")


# --------------------------------------------------------------------------------------
# Reference / seed data
# --------------------------------------------------------------------------------------

def build_countries() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"country_code": c, "country_name": v[0], "region": v[2], "currency_code": v[1]}
            for c, v in COUNTRIES.items()
        ]
    )


def build_vat_rates() -> pd.DataFrame:
    return pd.DataFrame(
        [{"country_code": c, "vat_class": cls, "vat_rate": r, "valid_from": vf, "valid_to": vt}
         for c, cls, r, vf, vt in VAT_RATES]
    )


def build_fx_rates() -> pd.DataFrame:
    """Daily EUR -> local rates with a light random walk. EUR row included at 1.0."""
    days = pd.date_range(HISTORY_START, EXTRACT_DATE, freq="D").date
    rows = []
    levels = {"PLN": 4.30, "RON": 4.9750, "EUR": 1.0}
    for d in days:
        for ccy, base in levels.items():
            if ccy == "EUR":
                rate = 1.0
            else:
                drift = rng.normal(0, 0.0035)
                levels[ccy] = float(np.clip(levels[ccy] * (1 + drift), base * 0.90, base * 1.10))
                rate = round(levels[ccy], 4)
            rows.append({
                "rate_date": d.isoformat(),
                "from_currency_code": "EUR",
                "to_currency_code": ccy,
                "exchange_rate": rate,
                "rate_source": "ECB_REFERENCE",
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------------------

def build_stores() -> pd.DataFrame:
    rows, sid = [], 1
    per_country = {"NL": 12, "DE": 15, "PL": 8, "RO": 6, "ES": 7}
    for cc, n in per_country.items():
        for i in range(n):
            city = CITIES[cc][i % len(CITIES[cc])]
            fmt = pick(STORE_FORMATS, p=[0.08, 0.46, 0.22, 0.24])
            opening = HISTORY_START - timedelta(days=int(rng.integers(200, 5200)))
            closing = None
            if rng.random() < 0.04:
                closing = HISTORY_START + timedelta(days=int(rng.integers(120, 600)))
            area = {"FLAGSHIP": (4200, 7000), "STANDARD": (1800, 3800),
                    "COMPACT": (600, 1500), "GARDEN_CENTRE": (2500, 9000)}[fmt]
            rows.append({
                "store_id": f"ST-{sid:04d}",
                "store_name": f"Verdanta {city}" + (f" {['Noord','Zuid','Centrum','Oost','West'][i % 5]}" if i >= len(CITIES[cc]) else ""),
                "store_format": fmt,
                "address_line_1": street(cc),
                "city": city,
                "postal_code": postal_code(cc),
                "country_code": cc,
                "latitude": city_coords(city)[0],
                "longitude": city_coords(city)[1],
                "selling_area_sqm": int(rng.integers(*area)),
                "store_manager_employee_id": None,   # backfilled after employees exist
                "opening_date": opening.isoformat(),
                "closing_date": closing.isoformat() if closing else None,
                "store_status": "CLOSED" if closing else "OPEN",
                "created_at": iso(ts(opening)),
                "updated_at": None,                  # backfilled
            })
            sid += 1
    return pd.DataFrame(rows)


def build_employees(stores: pd.DataFrame) -> pd.DataFrame:
    rows = []
    store_ids = stores["store_id"].tolist()
    store_country = dict(zip(stores["store_id"], stores["country_code"]))

    # Regional managers first (HQ, no store)
    eid = 1
    regional = []
    for region in sorted({v[2] for v in COUNTRIES.values()}):
        cc = next(c for c, v in COUNTRIES.items() if v[2] == region)
        emp_id = f"EMP-{eid:05d}"
        regional.append(emp_id)
        hire = HISTORY_START - timedelta(days=int(rng.integers(900, 4000)))
        rows.append(_employee_row(emp_id, cc, "Regional Manager", "Retail Operations",
                                  None, None, hire, None))
        eid += 1

    # Store managers
    managers = {}
    for st in store_ids:
        emp_id = f"EMP-{eid:05d}"
        cc = store_country[st]
        hire = HISTORY_START - timedelta(days=int(rng.integers(200, 3000)))
        rows.append(_employee_row(emp_id, cc, "Store Manager", "Retail Operations",
                                  st, pick(regional), hire, None))
        managers[st] = emp_id
        eid += 1

    # Store staff
    titles = ["Sales Associate", "Cashier", "Plant Specialist", "Warehouse Operative", "Assistant Manager"]
    while eid <= N_EMPLOYEES:
        st = pick(store_ids)
        cc = store_country[st]
        emp_id = f"EMP-{eid:05d}"
        hire = HISTORY_START - timedelta(days=int(rng.integers(-500, 2200)))
        term = None
        if rng.random() < 0.14:
            term = hire + timedelta(days=int(rng.integers(120, 900)))
            if term > EXTRACT_DATE:
                term = None
        rows.append(_employee_row(emp_id, cc, pick(titles, p=[0.42, 0.24, 0.12, 0.15, 0.07]),
                                  "Retail Operations", st, managers[st], hire, term))
        eid += 1

    df = pd.DataFrame(rows)

    # DEFECT: trailing whitespace on ~1.2% of names (classic CSV-from-legacy-ERP artefact)
    mask = rng.random(len(df)) < 0.012
    df.loc[mask, "last_name"] = df.loc[mask, "last_name"] + "  "
    return df


def _employee_row(emp_id, cc, title, dept, store_id, manager_id, hire, term) -> dict:
    fn, ln = pick(FIRST_NAMES[cc]), pick(LAST_NAMES[cc])
    status = "TERMINATED" if term else "ACTIVE"
    return {
        "employee_id": emp_id,
        "first_name": fn,
        "last_name": ln,
        "work_email": f"{slugify(fn)}.{slugify(ln)}@verdanta-group.example",
        "job_title": title,
        "department": dept,
        "store_id": store_id,
        "manager_employee_id": manager_id,
        "country_code": cc,
        "hire_date": hire.isoformat(),
        "termination_date": term.isoformat() if term else None,
        "employment_status": status,
        "created_at": iso(ts(hire)),
        "updated_at": iso(ts(term if term else hire)),
    }


def build_suppliers() -> pd.DataFrame:
    rows = []
    names = THIRD_PARTY_BRANDS + [
        "Vantage Horticulture", "Elbe Metallwaren", "Silva Timber Works", "Iberia Ceramica",
        "Baltic Poly Group", "Rhone Outdoor", "Meridian Seeds", "Carpathia Wood",
        "Aegean Stoneware", "Danube Plastics", "Frisia Textiles", "Alpina Tools",
        "Norddeich Solar", "Vistula Steel", "Occitane Garden", "Zeeland Composites",
        "Bavaria Grillwerk", "Lusitania Plants", "Maros Irrigation", "Weser Chemicals",
        "Odra Fasteners", "Tagus Fibreglass", "Kempen Nurseries", "Sibiu Forestry",
        "Almeria Growers", "Twente Machinery", "Prut Agri", "Leuven Biotech",
        "Galicia Stone", "Pomerania Nets",
    ]
    for i, name in enumerate(names[:N_SUPPLIERS], start=1):
        cc = pick(list(COUNTRIES) + ["IT", "FR", "CZ", "PT"])
        onboard = HISTORY_START - timedelta(days=int(rng.integers(300, 4000)))
        rows.append({
            "supplier_id": f"SUP-{i:04d}",
            "supplier_name": name,
            "supplier_country_code": cc,
            "lead_time_days": int(rng.integers(3, 62)),
            "is_preferred_supplier": bool(rng.random() < 0.3),
            "payment_terms_days": int(pick([14, 30, 45, 60, 90], p=[0.1, 0.45, 0.2, 0.2, 0.05])),
            "onboarded_date": onboard.isoformat(),
            "supplier_status": "ACTIVE" if rng.random() > 0.07 else "INACTIVE",
            "created_at": iso(ts(onboard)),
            "updated_at": iso(ts(onboard + timedelta(days=int(rng.integers(0, 600))))),
        })
    return pd.DataFrame(rows)


def build_products(suppliers: pd.DataFrame) -> list[dict]:
    """PIM export: JSON Lines with a nested attributes object and a supplier array."""
    l2s = list(CATEGORY_TREE)
    sup_ids = suppliers["supplier_id"].tolist()
    records = []
    for i in range(1, N_PRODUCTS + 1):
        l2 = pick(l2s)
        l3 = pick(CATEGORY_TREE[l2])
        brand = pick(OWN_BRANDS + THIRD_PARTY_BRANDS,
                     p=[0.10] * len(OWN_BRANDS) + [0.06] * len(THIRD_PARTY_BRANDS))
        cost = float(np.round(np.exp(rng.normal(2.5, 0.95)), 2))
        margin = rng.uniform(0.32, 0.62)
        list_price = money(cost / (1 - margin))
        launch = HISTORY_START - timedelta(days=int(rng.integers(-400, 2500)))
        discontinued = None
        if rng.random() < 0.09:
            discontinued = launch + timedelta(days=int(rng.integers(200, 1400)))
            if discontinued > EXTRACT_DATE:
                discontinued = None
        seasonal = l2 in ("Plants & Seeds", "BBQ & Grilling", "Outdoor Furniture", "Watering & Irrigation")
        records.append({
            "product_id": f"SKU-{i:06d}",
            "product_name": f"{brand} {l3.split(' &')[0]} {pick(['Classic','Pro','Compact','XL','Eco','Premium','Essential','Heavy Duty'])} {int(rng.integers(10, 900))}",
            "brand": brand,
            "is_own_brand": brand in OWN_BRANDS,
            "category": {
                "category_l1": CATEGORY_L1,
                "category_l2": l2,
                "category_l3": l3,
            },
            "supplier_ids": pick(sup_ids, n=int(rng.integers(1, 3))),
            "unit_cost_eur": cost,
            "list_price_eur": list_price,
            "vat_class": "REDUCED" if l2 in ("Plants & Seeds",) else "STANDARD",
            "attributes": {
                "colour": pick(["Anthracite", "Natural", "Green", "Terracotta", "Black", "Grey", "Teak", None]),
                "material": pick(["Steel", "Aluminium", "Wood", "Plastic", "Ceramic", "Composite", None]),
                "weight_kg": round(float(np.round(np.exp(rng.normal(0.8, 1.1)), 2)), 2),
                "is_seasonal": bool(seasonal),
                "peak_season": pick(["SPRING", "SUMMER"]) if seasonal else None,
            },
            "launch_date": launch.isoformat(),
            "discontinued_date": discontinued.isoformat() if discontinued else None,
            "product_status": "DISCONTINUED" if discontinued else "ACTIVE",
            "created_at": iso(ts(launch)),
            "updated_at": iso(ts(launch + timedelta(days=int(rng.integers(0, 700))))),
        })
    return records


def build_customers() -> pd.DataFrame:
    ccs = country_codes_weighted(N_CUSTOMERS)
    rows = []
    for i, cc in enumerate(ccs, start=1):
        fn, ln = pick(FIRST_NAMES[cc]), pick(LAST_NAMES[cc])
        signup = HISTORY_START - timedelta(days=int(rng.integers(-700, 2000)))
        signup = min(signup, EXTRACT_DATE)
        tier = pick(LOYALTY_TIERS, p=[0.52, 0.28, 0.15, 0.05])
        updated = signup + timedelta(days=int(rng.integers(0, 700)))
        updated = min(updated, EXTRACT_DATE)
        rows.append({
            "customer_id": f"CUST-{i:06d}",
            "first_name": fn,
            "last_name": ln,
            "email": f"{slugify(fn)}.{slugify(ln)}{int(rng.integers(1, 9999))}@example.com",
            "phone_number": _phone(cc),
            "birth_date": (date(1950, 1, 1) + timedelta(days=int(rng.integers(0, 20000)))).isoformat(),
            "preferred_language": {"NL": "nl", "DE": "de", "PL": "pl", "RO": "ro", "ES": "es"}[cc],
            "loyalty_tier": tier,                    # SCD2 attribute
            "loyalty_points_balance": int(rng.integers(0, 25000)),
            "marketing_opt_in": bool(rng.random() < 0.55),
            "address_line_1": street(cc),
            "city": pick(CITIES[cc]),                # SCD2 attribute
            "postal_code": postal_code(cc),
            "country_code": cc,                      # SCD2 attribute
            "customer_status": pick(["ACTIVE", "INACTIVE", "CLOSED"], p=[0.88, 0.09, 0.03]),
            "signup_date": signup.isoformat(),
            "created_at": iso(ts(signup)),
            "updated_at": iso(ts(updated)),
        })
    df = pd.DataFrame(rows)

    # ---- deliberate defects -----------------------------------------------------------
    # 1. ~2% missing email
    df.loc[rng.random(len(df)) < 0.02, "email"] = None
    # 2. mixed boolean encodings (source system upgraded mid-life)
    enc = rng.random(len(df))
    df["marketing_opt_in"] = df["marketing_opt_in"].astype(str)
    df.loc[enc < 0.20, "marketing_opt_in"] = df.loc[enc < 0.20, "marketing_opt_in"].map({"True": "Y", "False": "N"})
    df.loc[(enc >= 0.20) & (enc < 0.35), "marketing_opt_in"] = df.loc[(enc >= 0.20) & (enc < 0.35), "marketing_opt_in"].map({"True": "1", "False": "0"})
    df.loc[df["marketing_opt_in"].isin(["True", "False"]), "marketing_opt_in"] = df.loc[
        df["marketing_opt_in"].isin(["True", "False"]), "marketing_opt_in"
    ].str.lower()
    # 3. inconsistent country casing on ~1.5%
    m = rng.random(len(df)) < 0.015
    df.loc[m, "country_code"] = df.loc[m, "country_code"].str.lower()
    # 4. 35 hard duplicates under new customer_ids (same person re-registered)
    dupes = df.sample(35, random_state=SEED).copy()
    dupes["customer_id"] = [f"CUST-{N_CUSTOMERS + i:06d}" for i in range(1, 36)]
    df = pd.concat([df, dupes], ignore_index=True)
    return df


def _phone(cc: str) -> str:
    prefix = {"NL": "+31", "DE": "+49", "PL": "+48", "RO": "+40", "ES": "+34"}[cc]
    digits = "".join(str(int(d)) for d in rng.integers(0, 10, 9))
    style = rng.random()
    if style < 0.4:
        return f"{prefix}{digits}"
    if style < 0.7:
        return f"{prefix} {digits[:3]} {digits[3:6]} {digits[6:]}"
    if style < 0.9:
        return f"0{digits}"
    return f"({prefix}) {digits}"


# --------------------------------------------------------------------------------------
# Facts
# --------------------------------------------------------------------------------------

def build_orders(customers: pd.DataFrame, stores: pd.DataFrame,
                 employees: pd.DataFrame, products: list[dict], fx: pd.DataFrame):
    cust = customers[["customer_id", "country_code"]].copy()
    cust["country_code"] = cust["country_code"].str.upper()
    cust_by_country = {cc: g["customer_id"].tolist() for cc, g in cust.groupby("country_code")}

    open_stores = stores[stores["store_status"] == "OPEN"]
    stores_by_country = {cc: g["store_id"].tolist() for cc, g in open_stores.groupby("country_code")}
    staff = employees[employees["store_id"].notna()]
    staff_by_store = {st: g["employee_id"].tolist() for st, g in staff.groupby("store_id")}

    prod_lookup = {p["product_id"]: p for p in products}
    prod_ids = list(prod_lookup)
    # seasonal weighting of product choice
    seasonal_ids = [p["product_id"] for p in products if p["attributes"]["is_seasonal"]]

    # order date distribution driven by seasonality
    all_days = pd.date_range(HISTORY_START, EXTRACT_DATE, freq="D").date
    weights = np.array([SEASONALITY[d.month] * (1.35 if d.weekday() >= 5 else 1.0) for d in all_days])
    weights = weights / weights.sum()
    # sorted so that order_id is monotonic in time, as in a real ERP sequence
    order_days = np.sort(rng.choice(len(all_days), size=N_ORDERS, p=weights))

    order_rows, item_rows, return_rows = [], [], []
    item_seq, return_seq = 0, 0

    for oid in range(1, N_ORDERS + 1):
        d = all_days[order_days[oid - 1]]
        cc = country_codes_weighted(1)[0]
        currency = COUNTRIES[cc][1]
        channel = pick(CHANNELS, p=[0.58, 0.36, 0.06])

        store_id, employee_id = None, None
        if channel == "STORE" and stores_by_country.get(cc):
            store_id = pick(stores_by_country[cc])
            if staff_by_store.get(store_id):
                employee_id = pick(staff_by_store[store_id])

        customer_id = pick(cust_by_country.get(cc, cust["customer_id"].tolist()))
        # ~11% of store walk-ins are anonymous
        if channel == "STORE" and rng.random() < 0.11:
            customer_id = None

        order_ts_ = ts(d)
        status = pick(["COMPLETED", "CANCELLED", "PENDING", "REFUNDED"], p=[0.905, 0.035, 0.02, 0.04])

        n_lines = int(min(rng.geometric(0.42), 9))
        lines = []
        for ln in range(1, n_lines + 1):
            pid = pick(seasonal_ids) if (rng.random() < 0.45 and seasonal_ids) else pick(prod_ids)
            p = prod_lookup[pid]
            qty = int(min(rng.geometric(0.55), 12))
            fx_rate = _fx(fx, d, currency)
            unit_price_local = money(p["list_price_eur"] * fx_rate * float(rng.uniform(0.97, 1.06)))
            disc_pct = float(pick([0.0, 0.05, 0.10, 0.15, 0.25], p=[0.68, 0.10, 0.11, 0.07, 0.04]))
            gross = money(unit_price_local * qty)
            discount = money(gross * disc_pct)
            net = money(gross - discount)
            vr = vat_rate_for(cc, p["vat_class"], d)
            vat = money(net * vr)
            item_seq += 1
            lines.append({
                "order_item_id": f"OI-{item_seq:09d}",
                "order_id": f"ORD-{oid:08d}",
                "line_number": ln,
                "product_id": pid,
                "quantity": qty,
                "unit_price_local": unit_price_local,
                "discount_amount_local": discount,
                "line_gross_amount_local": gross,
                "line_net_amount_local": net,
                "vat_rate": vr,
                "vat_amount_local": vat,
                "unit_cost_eur": p["unit_cost_eur"],
                "currency_code": currency,
                "created_at": iso(order_ts_),
                "updated_at": iso(order_ts_),
            })

        gross_t = money(sum(l["line_gross_amount_local"] for l in lines))
        disc_t = money(sum(l["discount_amount_local"] for l in lines))
        net_t = money(sum(l["line_net_amount_local"] for l in lines))
        vat_t = money(sum(l["vat_amount_local"] for l in lines))

        # DEFECT: ~0.3% of headers do not reconcile to their lines (ERP rounding bug)
        if rng.random() < 0.003:
            net_t = money(net_t * float(rng.uniform(0.94, 1.06)))

        updated = order_ts_
        if status in ("REFUNDED", "CANCELLED"):
            updated = order_ts_ + timedelta(days=int(rng.integers(1, 21)))

        order_rows.append({
            "order_id": f"ORD-{oid:08d}",
            "order_number": f"{cc}-{d.year}-{oid:08d}",
            "customer_id": customer_id,
            "store_id": store_id,
            "employee_id": employee_id,
            "sales_channel": channel,
            "order_ts": iso(order_ts_),
            "order_status": status,
            "country_code": cc,
            "shipping_country_code": cc if rng.random() > 0.03 else pick(list(COUNTRIES)),
            "currency_code": currency,
            "payment_method": pick(PAYMENT_METHODS),
            "promotion_code": pick(["", "", "", "", "SPRING10", "GARDEN15", "NEWSLETTER5", "BLACKFRI25"]),
            "order_gross_amount": gross_t,
            "order_discount_amount": disc_t,
            "order_net_amount": net_t,
            "order_vat_amount": vat_t,
            "created_at": iso(order_ts_),
            "updated_at": iso(updated),
        })
        item_rows.extend(lines)

        # Returns: ~8% of completed orders have at least one returned line
        if status == "COMPLETED" and rng.random() < 0.08:
            line = pyrng.choice(lines)
            return_seq += 1
            rdate = d + timedelta(days=int(rng.integers(1, 45)))
            if rdate <= EXTRACT_DATE:
                qty_ret = int(rng.integers(1, line["quantity"] + 1))
                r_created = ts(rdate)
                unit_net = line["line_net_amount_local"] / max(line["quantity"], 1)
                return_rows.append({
                    "return_id": f"RET-{return_seq:07d}",
                    "order_id": line["order_id"],
                    "order_item_id": line["order_item_id"],
                    "product_id": line["product_id"],
                    "return_store_id": store_id if store_id else pick(stores_by_country.get(cc, [None])),
                    "return_ts": ts(rdate).strftime("%d/%m/%Y %H:%M"),   # DEFECT: non-ISO date format
                    "return_reason_code": pick(RETURN_REASONS, p=[0.12, 0.15, 0.38, 0.08, 0.19, 0.08]),
                    "quantity_returned": qty_ret,
                    "refund_amount_local": money(unit_net * qty_ret),
                    "currency_code": line["currency_code"],
                    "restock_flag": bool(rng.random() < 0.72),
                    "created_at": iso(r_created),
                    "updated_at": iso(r_created + timedelta(minutes=int(rng.integers(0, 2880)))),
                })

    orders = pd.DataFrame(order_rows)
    items = pd.DataFrame(item_rows)
    returns = pd.DataFrame(return_rows)

    # DEFECT: ~0.1% duplicated order headers (extract job re-ran and appended)
    dup = orders.sample(max(int(len(orders) * 0.001), 1), random_state=SEED)
    orders = pd.concat([orders, dup], ignore_index=True)

    # DEFECT: a handful of orders reference customers that are not in the CRM snapshot
    ghost = orders.sample(40, random_state=SEED + 1).index
    orders.loc[ghost, "customer_id"] = [f"CUST-{900000 + i:06d}" for i in range(len(ghost))]

    # DEFECT: a few zero-quantity lines
    z = items.sample(25, random_state=SEED + 2).index
    items.loc[z, "quantity"] = 0

    return orders, items, returns


def _fx(fx: pd.DataFrame, d: date, ccy: str) -> float:
    if ccy == "EUR":
        return 1.0
    if not hasattr(_fx, "_cache"):
        _fx._cache = {
            (r.rate_date, r.to_currency_code): r.exchange_rate
            for r in fx.itertuples()
        }
    return float(_fx._cache.get((d.isoformat(), ccy), 1.0))


# --------------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------------

@dataclass
class Extract:
    system: str
    entity: str
    fmt: str


def write_csv(df: pd.DataFrame, path: Path, sep: str = ","):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, sep=sep, na_rep="")


def write_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")


def write_jsonl(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def landing_path(out: Path, system: str, entity: str, ingest_date: date, filename: str) -> Path:
    return out / system / entity / f"ingest_date={ingest_date.isoformat()}" / filename


def main(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    landing = out_dir / "landing"
    seeds = out_dir / "seeds"

    print("building reference data ...")
    countries = build_countries()
    vat = build_vat_rates()
    fx = build_fx_rates()

    print("building dimensions ...")
    stores = build_stores()
    employees = build_employees(stores)
    # backfill store managers now that employees exist
    mgr = employees[employees["job_title"] == "Store Manager"].set_index("store_id")["employee_id"]
    stores["store_manager_employee_id"] = stores["store_id"].map(mgr)
    stores["updated_at"] = stores["created_at"]

    suppliers = build_suppliers()
    products = build_products(suppliers)
    customers = build_customers()

    print("building facts (this takes ~30s) ...")
    orders, items, returns = build_orders(customers, stores, employees, products, fx)

    # split: initial full extract vs daily incrementals
    order_date = pd.to_datetime(orders["order_ts"]).dt.date
    hist_mask = order_date <= HISTORY_END
    orders_hist, orders_inc = orders[hist_mask], orders[~hist_mask]
    items_hist = items[items["order_id"].isin(orders_hist["order_id"])]
    items_inc = items[items["order_id"].isin(orders_inc["order_id"])]
    ret_date = pd.to_datetime(returns["created_at"]).dt.date
    returns_hist, returns_inc = returns[ret_date <= HISTORY_END], returns[ret_date > HISTORY_END]

    print("writing seeds ...")
    write_csv(countries, seeds / "seed_countries.csv")
    write_csv(vat, seeds / "seed_vat_rates.csv")
    write_csv(fx, seeds / "seed_fx_rates_eur.csv")

    print("writing initial full extracts ...")
    d0 = HISTORY_END + timedelta(days=1)
    write_csv(customers, landing_path(landing, "crm", "customers", d0, f"customers_{d0:%Y%m%d}.csv"))
    write_csv(employees, landing_path(landing, "hr", "employees", d0, f"employees_{d0:%Y%m%d}.csv"))
    write_jsonl(products, landing_path(landing, "pim", "products", d0, f"products_{d0:%Y%m%d}.jsonl"))
    write_csv(suppliers, landing_path(landing, "pim", "suppliers", d0, f"suppliers_{d0:%Y%m%d}.csv"))
    write_csv(stores, landing_path(landing, "reference", "stores", d0, f"stores_{d0:%Y%m%d}.csv"))
    write_csv(orders_hist, landing_path(landing, "erp", "orders", d0, f"orders_{d0:%Y%m%d}.csv"))
    write_parquet(items_hist, landing_path(landing, "erp", "order_items", d0, f"order_items_{d0:%Y%m%d}.parquet"))
    write_csv(returns_hist, landing_path(landing, "erp", "returns", d0, f"returns_{d0:%Y%m%d}.csv"))

    print("writing daily incremental extracts ...")
    for i in range(INCREMENTAL_DAYS):
        d = HISTORY_END + timedelta(days=i + 1)
        if d > EXTRACT_DATE:
            break
        o = orders_inc[pd.to_datetime(orders_inc["order_ts"]).dt.date == d]
        it = items_inc[items_inc["order_id"].isin(o["order_id"])]
        r = returns_inc[pd.to_datetime(returns_inc["created_at"]).dt.date == d]
        if i > 0:  # d0 already written above as part of the full extract
            write_csv(o, landing_path(landing, "erp", "orders", d, f"orders_{d:%Y%m%d}.csv"))
            write_parquet(it, landing_path(landing, "erp", "order_items", d, f"order_items_{d:%Y%m%d}.parquet"))
            write_csv(r, landing_path(landing, "erp", "returns", d, f"returns_{d:%Y%m%d}.csv"))

    summary = {
        "company": COMPANY,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "seed": SEED,
        "history_window": f"{HISTORY_START} .. {EXTRACT_DATE}",
        "row_counts": {
            "countries": len(countries), "vat_rates": len(vat), "fx_rates": len(fx),
            "stores": len(stores), "employees": len(employees), "suppliers": len(suppliers),
            "products": len(products), "customers": len(customers),
            "orders": len(orders), "order_items": len(items), "returns": len(returns),
        },
    }
    (out_dir / "_generation_manifest.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./verdanta-source-data")
    args = ap.parse_args()
    main(Path(args.out))
