import os
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract, lit, sum as Fsum, count as Fcount
)

# ---- Influx config from env ----
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "esilv")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "public_transport")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")

RAW_BASE = os.getenv("HDFS_BASE", "/data/pt/raw/source=idfm")
MEASUREMENT = "pt_analytics_hourly"


def main():
    if not INFLUX_TOKEN:
        raise SystemExit("INFLUX_TOKEN not set. Export it or source your .env before spark-submit.")

    spark = (
        SparkSession.builder
        .appName("pt_hourly_metrics")
        .getOrCreate()
    )

    # Read raw files as binary so we can easily get file sizes and paths
    # This avoids expensive JSON parsing for huge payloads.
    HDFS_NN = os.getenv("HDFS_NN", "hdfs://master:9000")
    df = spark.read.format("binaryFile").load(f"{HDFS_NN}{RAW_BASE}/dt=*/hr=*/idfm_*.json")

    print("[DEBUG] sample path =", f"{HDFS_NN}{RAW_BASE}/dt=*/hr=*/idfm_*.json")
    print("[DEBUG] files_read =", df.count())


    # Extract dt and hr from file path: .../dt=YYYY-MM-DD/hr=HH/...
    df2 = (
        df.withColumn("path", input_file_name())
          .withColumn("dt", regexp_extract(col("path"), r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))
          .withColumn("hr", regexp_extract(col("path"), r"hr=([0-9]{2})", 1))
          .withColumn("source", lit("idfm"))
          .withColumn("feed", lit("next_departures_global"))
    )

    agg = (
        df2.groupBy("dt", "hr", "source", "feed")
           .agg(
               Fcount(lit(1)).alias("files_count"),
               Fsum(col("length")).alias("total_bytes")
           )
           .orderBy(col("dt").desc(), col("hr").desc())
    )

    rows = agg.collect()

    now = datetime.now(timezone.utc)

    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    from influxdb_client.client.write_api import SYNCHRONOUS
    write_api = influx.write_api(write_options=SYNCHRONOUS)

    influx.close()


    points = []
    for r in rows:
        # Timestamp for the hour bucket (UTC)
        ts = datetime.strptime(f"{r['dt']} {r['hr']}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)

        p = (
            Point(MEASUREMENT)
            .tag("source", r["source"])
            .tag("feed", r["feed"])
            .tag("dt", r["dt"])
            .tag("hr", r["hr"])
            .field("files_count", int(r["files_count"]))
            .field("total_bytes", int(r["total_bytes"]))
            .time(ts, WritePrecision.S)
        )
        points.append(p)

    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    print(f"[OK] wrote {len(points)} points to Influx measurement={MEASUREMENT} bucket={INFLUX_BUCKET}")

    spark.stop()


if __name__ == "__main__":
    main()
