# Databricks notebook source
# MAGIC %md
# MAGIC # CTA Bus Analytics — Dashboard Queries
# MAGIC
# MAGIC Run this notebook then use **"Open in a dashboard"** from the output tables
# MAGIC to build a Databricks Lakeview dashboard.

# COMMAND ----------

# DBTITLE 1, Network overview (latest hour)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     MAX(hour)                               AS as_of_hour,
# MAGIC     COUNT(DISTINCT route)                   AS routes_active,
# MAGIC     SUM(unique_vehicles)                    AS total_buses,
# MAGIC     ROUND(AVG(avg_speed_mph), 1)            AS network_avg_speed_mph,
# MAGIC     ROUND(AVG(delay_rate_pct), 1)           AS network_delay_rate_pct
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC WHERE hour = (SELECT MAX(hour) FROM workspace.gold_cta.agg_route_performance)

# COMMAND ----------

# DBTITLE 1, Route performance — latest hour (bar chart: speed by route)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY avg_speed_mph ASC) AS rank,
# MAGIC     route,
# MAGIC     route_name,
# MAGIC     route_color,
# MAGIC     avg_speed_mph,
# MAGIC     median_speed_mph,
# MAGIC     delay_rate_pct,
# MAGIC     unique_vehicles,
# MAGIC     observation_count
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC WHERE hour = (SELECT MAX(hour) FROM workspace.gold_cta.agg_route_performance)
# MAGIC   AND observation_count >= 5
# MAGIC ORDER BY avg_speed_mph ASC

# COMMAND ----------

# DBTITLE 1, Most delayed routes — latest hour (bar chart: delay % by route)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     route,
# MAGIC     route_name,
# MAGIC     delay_rate_pct,
# MAGIC     delayed_obs_count,
# MAGIC     observation_count,
# MAGIC     avg_speed_mph
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC WHERE hour = (SELECT MAX(hour) FROM workspace.gold_cta.agg_route_performance)
# MAGIC   AND observation_count >= 5
# MAGIC ORDER BY delay_rate_pct DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1, Network speed & delay over time (line chart)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     hour,
# MAGIC     ROUND(AVG(avg_speed_mph), 1)    AS network_avg_speed_mph,
# MAGIC     ROUND(AVG(delay_rate_pct), 1)   AS network_delay_rate_pct,
# MAGIC     SUM(unique_vehicles)             AS total_active_buses
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour

# COMMAND ----------

# DBTITLE 1, Top 10 busiest routes — latest hour (bar chart: vehicles)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     route,
# MAGIC     route_name,
# MAGIC     unique_vehicles,
# MAGIC     avg_speed_mph,
# MAGIC     delay_rate_pct
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC WHERE hour = (SELECT MAX(hour) FROM workspace.gold_cta.agg_route_performance)
# MAGIC ORDER BY unique_vehicles DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1, Route performance heatmap — speed by route by hour
# MAGIC %sql
# MAGIC SELECT
# MAGIC     hour,
# MAGIC     route_name,
# MAGIC     avg_speed_mph,
# MAGIC     delay_rate_pct,
# MAGIC     unique_vehicles
# MAGIC FROM workspace.gold_cta.agg_route_performance
# MAGIC WHERE route_name IS NOT NULL
# MAGIC   AND observation_count >= 3
# MAGIC ORDER BY hour, route_name
