# Databricks notebook source
# MAGIC %md
# MAGIC # CTA Bus Tracker — Raw Ingestion
# MAGIC
# MAGIC Polls the CTA Bus Tracker API and lands raw responses into Unity Catalog
# MAGIC as append-only Delta tables in the `raw` layer.
# MAGIC
# MAGIC **Endpoints covered:**
# MAGIC - `getvehicles` — real-time bus positions (run every 1–5 min)
# MAGIC - `getroutes`   — static route list (run daily or on-demand)
# MAGIC
# MAGIC **Target catalog layout:**
# MAGIC ```
# MAGIC <catalog>.raw_cta.bus_vehicles
# MAGIC <catalog>.raw_cta.bus_routes
# MAGIC ```
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Store your API key in Databricks Secrets:
# MAGIC    ```sh
# MAGIC    databricks secrets create-scope cta
# MAGIC    databricks secrets put-secret cta bus_tracker_key --string-value "<your_key>"
# MAGIC    ```
# MAGIC 2. The Unity Catalog and schema must exist (see cell below).

# COMMAND ----------

import requests

WORKSPACE_URL = "https://dbc-86881c45-3dc9.cloud.databricks.com/"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {TOKEN}"}

# Scope already exists, skip creation. Just put the secret:
r2 = requests.post(f"{WORKSPACE_URL}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "cta", "key": "bus_tracker_key", "string_value": "GAqivHfPULJTGxLTC6KL4zKCe"}
)
print("Put secret:", r2.status_code, r2.text)

# COMMAND ----------

# DBTITLE 1, Parameters (override via Databricks job widgets)
dbutils.widgets.text("catalog",       "workspace",  "Unity Catalog name")
dbutils.widgets.text("schema",        "raw_cta",    "Target schema")
dbutils.widgets.text("routes_filter", "",           "Comma-separated route IDs to filter (blank = all)")

CATALOG       = dbutils.widgets.get("catalog")
SCHEMA        = dbutils.widgets.get("schema")
ROUTES_FILTER = dbutils.widgets.get("routes_filter")  # e.g. "22,36,77"

# API key stored in Databricks Secrets — never hard-code credentials
API_KEY       = dbutils.secrets.get(scope="cta", key="bus_tracker_key")
BASE_URL      = "https://www.ctabustracker.com/bustime/api/v2"

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Route filter: {ROUTES_FILTER or '(all routes)'}")

# COMMAND ----------

# DBTITLE 1, Ensure catalog + schema exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, MapType
)

# ------------------------------------------------------------------
# Schema for the raw landing tables.
# We keep the raw JSON payload intact so nothing is lost on ingestion.
# ------------------------------------------------------------------

RAW_SCHEMA = StructType([
    StructField("ingested_at",    TimestampType(), nullable=False),
    StructField("endpoint",       StringType(),    nullable=False),
    StructField("http_status",    StringType(),    nullable=True),
    StructField("raw_json",       StringType(),    nullable=False),   # full API response
])

# COMMAND ----------

# DBTITLE 1, Helper — call CTA Bus Tracker API

def call_cta(endpoint: str, params: dict) -> dict:
    """
    Call a CTA Bus Tracker API endpoint.
    Returns the parsed JSON body.
    Raises on HTTP errors; CTA API errors are preserved in the response.
    """
    params = {
        "key":    API_KEY,
        "format": "json",
        **params,
    }
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

# DBTITLE 1, Ingest — getvehicles

def ingest_vehicles():
    """
    Fetch real-time bus positions and append to raw_cta.bus_vehicles.
    CTA limits to 10 routes per request, so we page automatically.
    """
    ingested_at = datetime.now(timezone.utc)
    rows = []

    # Resolve routes to query
    if ROUTES_FILTER:
        route_ids = [r.strip() for r in ROUTES_FILTER.split(",") if r.strip()]
    else:
        # Fetch all routes first, then query vehicles for each batch
        routes_resp = call_cta("getroutes", {})
        route_ids = [
            r["rt"]
            for r in routes_resp.get("bustime-response", {}).get("routes", [])
        ]

    # CTA allows max 10 routes per getvehicles request
    BATCH_SIZE = 10
    for i in range(0, len(route_ids), BATCH_SIZE):
        batch = route_ids[i : i + BATCH_SIZE]
        params = {"rt": ",".join(batch)}
        try:
            body = call_cta("getvehicles", params)
            http_status = "200"
        except requests.HTTPError as exc:
            body = {"error": str(exc)}
            http_status = str(exc.response.status_code) if exc.response else "unknown"

        rows.append(Row(
            ingested_at = ingested_at,
            endpoint    = "getvehicles",
            http_status = http_status,
            raw_json    = json.dumps(body),
        ))

    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    (
        df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.bus_vehicles")
    )
    print(f"[getvehicles] Appended {len(rows)} batch row(s) at {ingested_at.isoformat()}")


ingest_vehicles()

# COMMAND ----------

# DBTITLE 1, Ingest — getroutes (static, run daily)

def ingest_routes():
    """
    Fetch the full list of CTA bus routes and append to raw_cta.bus_routes.
    This is relatively static — run once daily or on-demand.
    """
    ingested_at = datetime.now(timezone.utc)

    try:
        body = call_cta("getroutes", {})
        http_status = "200"
    except requests.HTTPError as exc:
        body = {"error": str(exc)}
        http_status = str(exc.response.status_code) if exc.response else "unknown"

    df = spark.createDataFrame(
        [Row(
            ingested_at = ingested_at,
            endpoint    = "getroutes",
            http_status = http_status,
            raw_json    = json.dumps(body),
        )],
        schema=RAW_SCHEMA,
    )
    (
        df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.bus_routes")
    )
    print(f"[getroutes] Appended 1 row at {ingested_at.isoformat()}")


ingest_routes()

# COMMAND ----------

# DBTITLE 1, Quick sanity check

display(spark.sql(f"""
    SELECT
        endpoint,
        COUNT(*)                                  AS batch_rows,
        MAX(ingested_at)                          AS latest_ingestion
    FROM {CATALOG}.{SCHEMA}.bus_vehicles
    GROUP BY endpoint
"""))

display(spark.sql(f"""
    SELECT ingested_at, http_status,
           json_array_length(
               get_json_object(raw_json, '$.bustime-response.routes')
           ) AS route_count
    FROM {CATALOG}.{SCHEMA}.bus_routes
    ORDER BY ingested_at DESC
    LIMIT 5
"""))
