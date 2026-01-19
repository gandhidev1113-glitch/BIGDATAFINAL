import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract,
    to_timestamp, substring, lit, coalesce, explode
)
from influxdb_client import InfluxDBClient, Point, WritePrecision
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

IDFM_SCHEMA = StructType([
    StructField("LineRef", StructType([
        StructField("value", StringType(), True)
    ]), True),

    StructField("VehicleMode", ArrayType(StringType()), True)
])

# ---- Influx settings (from env) ----
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "esilv")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "public_transport")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")

MEASUREMENT = "pt_line_hourly"

HDFS_BASE = os.getenv("HDFS_BASE", "/data/pt/raw/source=idfm")
FEED = os.getenv("FEED", "next_departures_global")
SOURCE = os.getenv("SOURCE", "idfm")

if not INFLUX_TOKEN:
    raise SystemExit("INFLUX_TOKEN not set. source your influx.env before spark-submit.")


def main():
    spark = (
        SparkSession.builder
        .appName("pt_line_hourly_metrics")
        .getOrCreate()
    )

    # We read JSON and extract:
    # - dt / hr from file path
    # - LineRef.value
    # - VehicleMode (array) -> explode to count mode activity

    # Only recent files (you can widen this if you want)
    path = f"{HDFS_BASE}/dt=*/hr=*/idfm_*.json"

    df = spark.read.schema(IDFM_SCHEMA) \
         .option("multiLine", "false") \
         .json(path)

    # Add filepath for dt/hr extraction
    df = df.withColumn("path", input_file_name())
    df = df.withColumn("dt", regexp_extract(col("path"), r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))
    df = df.withColumn("hr", regexp_extract(col("path"), r"hr=([0-9]{1,2})", 1))

    # LineRef is nested like LineRef.value
    df = df.withColumn("line", col("LineRef.value"))
   
    # --- DEBUG (temporary) ---
    print("INPUT PATH =", path)
    print("rows read =", df.count())
    print("non-null line =", df.filter(col("line").isNotNull()).count())
    df.select("dt", "hr", "line", "VehicleMode").show(10, truncate=False)
    # --- END DEBUG ---

    # Some records have VehicleMode as [] or missing
    # We'll create a "mode" row per element in VehicleMode, default to "UNKNOWN"
    df_modes = df.withColumn("mode_arr", coalesce(col("VehicleMode"), lit([])))
    df_modes = df_modes.withColumn("mode", explode(col("mode_arr")))
    df_modes = df_modes.withColumn("mode", coalesce(col("mode"), lit("UNKNOWN")))

    from pyspark.sql.functions import countDistinct, count, desc

    # --- Line activity (journeys) ---
    # Use DatedVehicleJourneyRef.value as a journey id if present; else fallback to DatedVehicleJourneyRef
   
    df_lines = (
        df
        .filter(col("line").isNotNull() & (col("line") != ""))
        .groupBy("dt", "hr", "line")
        .agg(count("*").alias("events_count"))
    )

    # Keep only top N lines per hour to avoid writing too many points
    TOP_N = 10
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    w = Window.partitionBy("dt", "hr").orderBy(desc("journeys_count"))
    df_lines_top = df_lines.withColumn("rn", row_number().over(w)).filter(col("rn") <= TOP_N).drop("rn")

    # --- Mode activity ---
    df_mode_counts = (
        df_modes
        .groupBy("dt", "hr", "mode")
        .agg(count("*").alias("events_count"))
    )

    

    # --- DEBUG (temporary) ---
    print("journeys_by_line rows =", journeys_by_line.count())
    journeys_by_line.show(20, truncate=False)
    # --- END DEBUG ---
    
    # Journeys per mode per hour
    journeys_by_mode = (
        df_modes.groupBy("dt", "hr", "mode")
        .count()
        .withColumnRenamed("count", "journeys_count")
    )

    # Collect small aggregates to driver for Influx write
    line_rows = journeys_by_line.collect()
    mode_rows = journeys_by_mode.collect()

    print("[DEBUG] df_lines_top rows =", df_lines_top.count())
    df_lines_top.show(5, truncate=False)

    print("[DEBUG] df_mode_counts rows =", df_mode_counts.count())
    df_mode_counts.show(5, truncate=False)

    points = []

    # Points for top lines
    for r in df_lines_top.collect():
        points.append(
            Point("pt_line_hourly")
            .tag("source", "idfm")
            .tag("line", r["line"])
            .field("journeys_count", int(r["journeys_count"]))
            .tag("dt", r["dt"])
            .tag("hr", str(r["hr"]))
        # optional: set timestamp to hour boundary if you want; otherwise omit
        )

    # Points for modes
    for r in df_mode_counts.collect():
        points.append(
            Point("pt_mode_hourly")
            .tag("source", "idfm")
            .tag("mode", r["mode"])
            .field("events_count", int(r["events_count"]))
            .tag("dt", r["dt"])
            .tag("hr", str(r["hr"]))
        )

    print(f"[DEBUG] total points prepared = {len(points)}")

    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    from influxdb_client.client.write_api import SYNCHRONOUS
    write_api = influx.write_api(write_options=SYNCHRONOUS)
    

    print("[DEBUG] result rows =", result_df.count())
    result_df.show(10, truncate=False)

    points = []
    # helper: build hour timestamp
    def hour_ts(dt_str, hr_str):
        # dt_str = YYYY-MM-DD, hr_str = 0-23
        return datetime.strptime(f"{dt_str} {int(hr_str):02d}:00:00", "%Y-%m-%d %H:%M:%S")

    for r in line_rows:
        if not r["dt"] or not r["hr"] or not r["line"]:
            continue
        ts = hour_ts(r["dt"], r["hr"])
        p = (
            Point(MEASUREMENT)
            .tag("source", SOURCE)
            .tag("feed", FEED)
            .tag("dt", r["dt"])
            .tag("hr", str(int(r["hr"])))
            .tag("line", r["line"])
            .field("journeys_count", int(r["journeys_count"]))
            .time(ts, WritePrecision.S)
        )
        points.append(p)

    for r in mode_rows:
        if not r["dt"] or not r["hr"] or not r["mode"]:
            continue
        ts = hour_ts(r["dt"], r["hr"])
        p = (
            Point(MEASUREMENT)
            .tag("source", SOURCE)
            .tag("feed", FEED)
            .tag("dt", r["dt"])
            .tag("hr", str(int(r["hr"])))
            .tag("mode", r["mode"])
            .field("journeys_count", int(r["journeys_count"]))
            .time(ts, WritePrecision.S)
        )
        points.append(p)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    print(f"[OK] wrote {len(points)} points to Influx (line + mode)")

    influx.close()
    spark.stop()


if __name__ == "__main__":
    main()
