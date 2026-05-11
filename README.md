# CTA Bus Analytics Pipeline

A real-time data pipeline that ingests Chicago Transit Authority (CTA) Bus Tracker API data into Databricks, transforms it through a medallion architecture (raw → stage → gold), and surfaces insights via a live dashboard.

---

## Architecture

```
CTA Bus Tracker API
        │
        ▼
┌───────────────────┐
│   RAW LAYER       │  workspace.raw_cta
│  (append-only)    │  bus_vehicles, bus_routes
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   STAGE LAYER     │  workspace.stage_cta
│  (typed columns)  │  bus_vehicle_positions, bus_routes
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   GOLD LAYER      │  workspace.gold_cta
│  (aggregated KPIs)│  agg_route_performance
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   DASHBOARD       │  CTA Bus Analytics Dashboard
│  (Lakeview)       │  Refreshes every 5 minutes
└───────────────────┘
```

---

## Notebooks

| Notebook | Layer | Description |
|---|---|---|
| `raw/01_ingest_cta_bus_raw.py` | Raw | Polls CTA Bus Tracker API (`getvehicles`, `getroutes`) and lands raw JSON responses into Unity Catalog |
| `stage/02_stage_cta_bus.py` | Stage | Parses and flattens raw JSON into typed Delta tables with one row per bus per poll |
| `gold/03_gold_agg_route_performance.py` | Gold | Aggregates staged positions into hourly route-level KPIs including derived speed, delay rate, and vehicle counts |
| `dashboard/04_dashboard_queries.py` | Dashboard | SQL queries powering the Lakeview dashboard |

---

## Data Flow

### Raw Layer
- **`raw_cta.bus_vehicles`** — full API response JSON per batch, appended every 5 minutes
- **`raw_cta.bus_routes`** — full route list JSON snapshot, appended every 5 minutes

### Stage Layer
- **`stage_cta.bus_vehicle_positions`** — one row per bus per poll, deduplicated on `(vehicle_id, vehicle_timestamp)`. Fields: `vehicle_id`, `vehicle_timestamp`, `route`, `destination`, `latitude`, `longitude`, `heading_degrees`, `pattern_dist_ft`, `is_delayed`
- **`stage_cta.bus_routes`** — one row per route (full refresh). Fields: `route_id`, `route_name`, `route_color`

### Gold Layer
- **`gold_cta.agg_route_performance`** — one row per route per hour. Fields:

| Field | Description |
|---|---|
| `hour` | Truncated to the hour |
| `route` / `route_name` | Route ID and name |
| `avg_speed_mph` | Average derived speed (calculated from `pattern_dist_ft` delta between readings) |
| `median_speed_mph` | Median speed |
| `delay_rate_pct` | % of readings where CTA flagged the bus as delayed |
| `unique_vehicles` | Number of distinct buses active on the route |
| `observation_count` | Total position readings that hour |

> **Note:** The CTA Bus Tracker API does not return a `spd` field for this account. Speed is derived as:
> $$\text{speed\_mph} = \frac{\Delta\text{pattern\_dist\_ft} / 5280}{\Delta\text{seconds} / 3600}$$

---

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- CTA Bus Tracker API key ([register here](https://www.ctabustracker.com/home))

### 1. Store API Key in Databricks Secrets
Run this once in any Databricks notebook:

```python
import requests

WORKSPACE_URL = "https://<your-workspace>.cloud.databricks.com"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {TOKEN}"}

# Create scope (skip if already exists)
requests.post(f"{WORKSPACE_URL}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": "cta", "initial_manage_principal": "users"}
)

# Store key
requests.post(f"{WORKSPACE_URL}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "cta", "key": "bus_tracker_key", "string_value": "<your_api_key>"}
)
```

**Delete this cell immediately after running.**

### 2. Upload Notebooks
Import each notebook into your Databricks workspace:
- `Workspace → Import → File` → upload each `.py` file

### 3. Create the Job
In **Jobs & Pipelines → Create Job**, add three tasks in order:

| Task | Notebook | Depends On | Parameters |
|---|---|---|---|
| `ingest_raw` | `01_ingest_cta_bus_raw` | — | `catalog=workspace`, `schema=raw_cta` |
| `stage` | `02_stage_cta_bus` | `ingest_raw` | `catalog=workspace`, `stage_schema=stage_cta` |
| `gold` | `03_gold_agg_route_performance` | `stage` | `catalog=workspace`, `stage_schema=stage_cta`, `gold_schema=gold_cta` |

Set schedule: **every 5 minutes**, timezone: **America/Chicago**

### 4. Create the Dashboard
Run `04_dashboard_queries.py` in Databricks, then add each cell's output as a visualization to a new Lakeview dashboard. Set the dashboard to **Schedule → every 5 minutes**.

---

## Dashboard Panels

| Panel | Chart Type | Description |
|---|---|---|
| Active Buses | Counter | Total buses currently on the network |
| Avg Speed (mph) | Counter | Network-wide average speed |
| Speed by Route | Horizontal Bar | All routes ranked by average speed (slowest first) |
| Most Delayed Routes | Horizontal Bar | Top 15 routes by delay rate % |
| Network Speed & Delay Over Time | Line | Hourly trend of speed and delay across all routes |
| Busiest Routes | Horizontal Bar | Top 10 routes by number of active vehicles |

---

## Project Structure

```
notebooks/
├── raw/
│   ├── 01_ingest_cta_bus_raw.py
│   └── job_cta_bus_raw_ingest.yml
├── stage/
│   └── 02_stage_cta_bus.py
├── gold/
│   └── 03_gold_agg_route_performance.py
└── dashboard/
    └── 04_dashboard_queries.py
```
