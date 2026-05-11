# Databricks notebook source
# MAGIC %md
# MAGIC # CTA Bus Tracker — Gold Layer: Route Performance
# MAGIC
# MAGIC Aggregates staged bus positions into hourly route-level KPIs.
# MAGIC
# MAGIC **Source:** `stage_cta.bus_vehicle_positions` + `stage_cta.bus_routes`
# MAGIC **Target:** `gold_cta.agg_route_performance`
# MAGIC
# MAGIC **Metrics per route per hour:**
# MAGIC - Active bus count
# MAGIC - Average & median speed
# MAGIC - Delay rate (%)
# MAGIC - Unique vehicles seen
# MAGIC - Observations (number of position readings)

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("catalog",       "workspace",   "Unity Catalog name")
dbutils.widgets.text("stage_schema",  "stage_cta",   "Stage schema")   # source: stage layer
dbutils.widgets.text("gold_schema",   "gold_cta",    "Gold schema")    # target: gold layer

CATALOG       = dbutils.widgets.get("catalog")
STAGE_SCHEMA  = dbutils.widgets.get("stage_schema")
GOLD_SCHEMA   = dbutils.widgets.get("gold_schema")

print(f"Source : {CATALOG}.{STAGE_SCHEMA}")
print(f"Target : {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# DBTITLE 1, Ensure gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# DBTITLE 1, Build agg_route_performance

from pyspark.sql.functions import (
    col, date_trunc, count, countDistinct,
    avg, round, sum, when, lit, percentile_approx,
    lag, unix_timestamp
)
from pyspark.sql.window import Window

positions = spark.table(f"{CATALOG}.{STAGE_SCHEMA}.bus_vehicle_positions")
routes    = spark.table(f"{CATALOG}.{STAGE_SCHEMA}.bus_routes")

# Derive speed from pdist delta between consecutive readings for each vehicle.
# speed_mph = (pdist_delta_ft / 5280) / (time_delta_seconds / 3600)
vehicle_window = Window.partitionBy("vehicle_id").orderBy("vehicle_timestamp")

positions = (
    positions
    .withColumn("prev_pdist",    lag("pattern_dist_ft").over(vehicle_window))
    .withColumn("prev_tmstmp",   lag("vehicle_timestamp").over(vehicle_window))
    .withColumn("pdist_delta_ft",
        when(
            col("pattern_dist_ft") > col("prev_pdist"),   # only when moving forward on route
            col("pattern_dist_ft") - col("prev_pdist")
        )
    )
    .withColumn("time_delta_sec",
        unix_timestamp("vehicle_timestamp") - unix_timestamp("prev_tmstmp")
    )
    .withColumn("derived_speed_mph",
        when(
            (col("time_delta_sec") > 0) & (col("pdist_delta_ft").isNotNull()),
            round((col("pdist_delta_ft") / 5280) / (col("time_delta_sec") / 3600), 1)
        )
    )
)

agg = (
    positions
    # Truncate to the hour for grouping
    .withColumn("hour", date_trunc("hour", col("vehicle_timestamp")))
    .groupBy("hour", "route")
    .agg(
        count("*")                                                      .alias("observation_count"),
        countDistinct("vehicle_id")                                    .alias("unique_vehicles"),
        round(avg("derived_speed_mph"), 1)                             .alias("avg_speed_mph"),
        percentile_approx("derived_speed_mph", 0.5).cast("double")    .alias("median_speed_mph"),
        round(
            avg(when(col("is_delayed") == True, 1).otherwise(0)) * 100, 1
        )                                                  .alias("delay_rate_pct"),
        sum(when(col("is_delayed") == True, 1).otherwise(0)).alias("delayed_obs_count"),
    )
    # Join to get route name and color
    .join(
        routes.select("route_id", "route_name", "route_color"),
        col("route") == col("route_id"),
        how="left"
    )
    .drop("route_id")
    .select(
        "hour",
        "route",
        "route_name",
        "route_color",
        "observation_count",
        "unique_vehicles",
        "avg_speed_mph",
        "median_speed_mph",
        "delay_rate_pct",
        "delayed_obs_count",
    )
    .orderBy("hour", "route")
)

TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.agg_route_performance"

(
    agg.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(TARGET_TABLE)
)

total = agg.count()
print(f"Written {total} rows to {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1, Top 10 slowest routes (latest hour)

display(spark.sql(f"""
    WITH latest AS (
        SELECT MAX(hour) AS max_hour FROM {CATALOG}.{GOLD_SCHEMA}.agg_route_performance
    )
    SELECT
        p.route,
        p.route_name,
        p.avg_speed_mph,
        p.median_speed_mph,
        p.delay_rate_pct,
        p.unique_vehicles,
        p.observation_count
    FROM {CATALOG}.{GOLD_SCHEMA}.agg_route_performance p
    CROSS JOIN latest
    WHERE p.hour = latest.max_hour
    ORDER BY p.avg_speed_mph ASC
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1, Most delayed routes (latest hour)

display(spark.sql(f"""
    WITH latest AS (
        SELECT MAX(hour) AS max_hour FROM {CATALOG}.{GOLD_SCHEMA}.agg_route_performance
    )
    SELECT
        p.route,
        p.route_name,
        p.delay_rate_pct,
        p.delayed_obs_count,
        p.observation_count,
        p.avg_speed_mph
    FROM {CATALOG}.{GOLD_SCHEMA}.agg_route_performance p
    CROSS JOIN latest
    WHERE p.hour = latest.max_hour
      AND p.observation_count >= 5      -- filter low-sample routes
    ORDER BY p.delay_rate_pct DESC
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1, Speed trend over time (all hours, all routes)

display(spark.sql(f"""
    SELECT
        hour,
        ROUND(AVG(avg_speed_mph), 1)    AS network_avg_speed_mph,
        ROUND(AVG(delay_rate_pct), 1)   AS network_delay_rate_pct,
        SUM(unique_vehicles)             AS total_active_buses
    FROM {CATALOG}.{GOLD_SCHEMA}.agg_route_performance
    GROUP BY hour
    ORDER BY hour
"""))
