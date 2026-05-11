# Databricks notebook source
# MAGIC %md
# MAGIC # CTA Bus Tracker — Stage Layer
# MAGIC
# MAGIC Reads raw JSON from `raw_cta` and flattens into typed columns in `stage_cta`.
# MAGIC
# MAGIC **Source → Target:**
# MAGIC - `raw_cta.bus_vehicles` → `stage_cta.bus_vehicle_positions` (one row per bus per poll)
# MAGIC - `raw_cta.bus_routes`   → `stage_cta.bus_routes` (one row per route, latest snapshot)

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("raw_schema",   "raw_cta",   "Source schema")
dbutils.widgets.text("stage_schema", "stage_cta", "Target schema")

CATALOG       = dbutils.widgets.get("catalog")
RAW_SCHEMA    = dbutils.widgets.get("raw_schema")
STAGE_SCHEMA  = dbutils.widgets.get("stage_schema")

print(f"Source : {CATALOG}.{RAW_SCHEMA}")
print(f"Target : {CATALOG}.{STAGE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1, Ensure stage schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{STAGE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1, Stage — bus_vehicle_positions
# One row per individual bus per ingestion poll.
# Merges on (vehicle_id, tmstmp) so re-runs are idempotent.

from pyspark.sql.functions import (
    col, explode, from_json, get_json_object,
    to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    ArrayType, StructType, StructField,
    StringType, BooleanType, DoubleType
)

VEHICLE_SCHEMA = ArrayType(StructType([
    StructField("vid",          StringType()),   # vehicle ID
    StructField("tmstmp",       StringType()),   # CTA timestamp string "YYYYMMDD HH:MM"
    StructField("lat",          StringType()),
    StructField("lon",          StringType()),
    StructField("hdg",          StringType()),   # heading in degrees
    StructField("rt",           StringType()),   # route (e.g. "22")
    StructField("des",          StringType()),   # destination/headsign
    StructField("spd",          DoubleType()),   # speed in mph (not always returned)
    StructField("pdist",        DoubleType()),   # distance along route pattern in feet
    StructField("dly",          BooleanType()),  # delayed flag
    StructField("tatripid",     StringType()),
    StructField("tablockid",    StringType()),
    StructField("zone",         StringType()),
]))

raw_vehicles = spark.table(f"{CATALOG}.{RAW_SCHEMA}.bus_vehicles")

staged_vehicles = (
    raw_vehicles
    .withColumn(
        "vehicles",
        from_json(
            get_json_object(col("raw_json"), "$.bustime-response.vehicle"),
            VEHICLE_SCHEMA
        )
    )
    .filter(col("vehicles").isNotNull())          # skip batches with no active buses
    .withColumn("v", explode(col("vehicles")))    # one row per bus
    .select(
        col("ingested_at"),
        col("v.vid").alias("vehicle_id"),
        to_timestamp(col("v.tmstmp"), "yyyyMMdd HH:mm").alias("vehicle_timestamp"),
        col("v.rt").alias("route"),
        col("v.des").alias("destination"),
        col("v.lat").cast("double").alias("latitude"),
        col("v.lon").cast("double").alias("longitude"),
        col("v.hdg").cast("integer").alias("heading_degrees"),
        col("v.spd").cast("double").alias("speed_mph"),        # null if not returned by API
        col("v.pdist").cast("double").alias("pattern_dist_ft"), # feet along route pattern
        col("v.dly").alias("is_delayed"),
        col("v.tatripid").alias("trip_id"),
        col("v.tablockid").alias("block_id"),
        col("v.zone").alias("zone"),
    )
    .dropDuplicates(["vehicle_id", "vehicle_timestamp"])  # idempotent
)

TARGET_TABLE = f"{CATALOG}.{STAGE_SCHEMA}.bus_vehicle_positions"

# Create table on first run, then merge to avoid duplicates on re-runs
if not spark.catalog.tableExists(TARGET_TABLE):
    (
        staged_vehicles.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"Created {TARGET_TABLE} with {staged_vehicles.count()} rows")
else:
    staged_vehicles.createOrReplaceTempView("staged_vehicles_vw")
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING staged_vehicles_vw s
        ON t.vehicle_id = s.vehicle_id AND t.vehicle_timestamp = s.vehicle_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged into {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1, Stage — bus_routes
# One row per route — overwrites with latest snapshot (routes rarely change).

ROUTE_SCHEMA = ArrayType(StructType([
    StructField("rt",    StringType()),   # route ID e.g. "22"
    StructField("rtnm",  StringType()),   # route name e.g. "Clark"
    StructField("rtclr", StringType()),   # hex color e.g. "#336633"
    StructField("rtdd",  StringType()),   # route display designation
]))

# Use only the single most recent snapshot
latest_routes_raw = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.bus_routes")
    .orderBy(col("ingested_at").desc())
    .limit(1)
)

staged_routes = (
    latest_routes_raw
    .withColumn(
        "routes",
        from_json(
            get_json_object(col("raw_json"), "$.bustime-response.routes"),
            ROUTE_SCHEMA
        )
    )
    .withColumn("r", explode(col("routes")))
    .select(
        col("ingested_at").alias("snapshot_at"),
        col("r.rt").alias("route_id"),
        col("r.rtnm").alias("route_name"),
        col("r.rtclr").alias("route_color"),
        col("r.rtdd").alias("route_display"),
    )
)

(
    staged_routes.write
    .format("delta")
    .mode("overwrite")           # full refresh — routes are a small static list
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{STAGE_SCHEMA}.bus_routes")
)
print(f"Wrote {staged_routes.count()} routes to {CATALOG}.{STAGE_SCHEMA}.bus_routes")

# COMMAND ----------

# DBTITLE 1, Sanity check

display(spark.sql(f"""
    SELECT
        route,
        COUNT(*)            AS position_records,
        MIN(vehicle_timestamp) AS earliest,
        MAX(vehicle_timestamp) AS latest,
        ROUND(AVG(speed_mph), 1) AS avg_speed_mph,
        SUM(CAST(is_delayed AS INT)) AS delayed_count
    FROM {CATALOG}.{STAGE_SCHEMA}.bus_vehicle_positions
    GROUP BY route
    ORDER BY position_records DESC
    LIMIT 20
"""))

display(spark.sql(f"""
    SELECT route_id, route_name, route_color
    FROM {CATALOG}.{STAGE_SCHEMA}.bus_routes
    ORDER BY route_id
    LIMIT 20
"""))
