#!/usr/bin/env python3
import os
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WriteOptions

# ----------------------------
# Config / Env
# ----------------------------
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

PRIM_API_KEY = os.getenv("PRIM_API_KEY")
IDFM_GLOBAL_URL = os.getenv("IDFM_GLOBAL_URL")  # e.g. https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable?LineRef=ALL
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.0.0.46:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pt.realtime.raw")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))  # set 120 to stay under daily limit comfortably

# HDFS base directory
HDFS_BASE = os.getenv("HDFS_BASE", "/data/pt/raw/source=idfm")

# Local temp directory
LOCAL_BASE = os.getenv("LOCAL_BASE", "/tmp/idfm_raw")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")

if not PRIM_API_KEY or not IDFM_GLOBAL_URL:
    raise SystemExit(
        "Missing PRIM_API_KEY or IDFM_GLOBAL_URL.\n"
        "Check ~/idfm_ingest/.env (and ensure it is in the same folder as this script)."
    )


# ----------------------------
# Helpers
# ----------------------------
def utc_now():
    return datetime.now(timezone.utc)

def hdfs_mkdir(path: str):
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", path], check=True)

def hdfs_put(local_file: str, hdfs_dir: str):
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_dir + "/"], check=True)

def fetch_idfm_json():
    headers = {
        "apiKey": PRIM_API_KEY,          # IMPORTANT: correct header name
        "Accept": "application/json",
    }
    r = requests.get(IDFM_GLOBAL_URL, headers=headers, timeout=90)
    r.raise_for_status()
    return r.json()


# ----------------------------
# Kafka Producer
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP.split(",") if s.strip()],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=50,
    acks="all",
    retries=3,
    request_timeout_ms=60000,
)

# =========================
# InfluxDB Client
# =========================
influx_write = None
if INFLUX_URL and INFLUX_ORG and INFLUX_BUCKET and INFLUX_TOKEN:
    influx = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )
    influx_write = influx.write_api(
        write_options=WriteOptions(batch_size=1)
    )
else:
    print("[WARN] InfluxDB not fully configured — metrics disabled")

def main():
    print("=== IDFM ingestion started ===")
    print(f"IDFM URL: {IDFM_GLOBAL_URL}")
    print(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"HDFS base: {HDFS_BASE}")
    print(f"Local base: {LOCAL_BASE}")
    print(f"Poll seconds: {POLL_SECONDS}")
    print("==============================")

    while True:
        start_time = time.time()
        now = utc_now()
        dt = now.strftime("%Y-%m-%d")
        hr = now.strftime("%H")
        ts = now.strftime("%Y%m%dT%H%M%SZ")

        # partition dirs
        local_dir = Path(f"{LOCAL_BASE}/dt={dt}/hr={hr}")
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / f"idfm_{ts}.json"

        hdfs_dir = f"{HDFS_BASE}/dt={dt}/hr={hr}"
        hdfs_path = f"{hdfs_dir}/{local_file.name}"

        try:
            # 1) Fetch
            t_fetch = time.time()
            data = fetch_idfm_json()
            fetch_ms = int((time.time() - t_fetch) * 1000)

            # 2) Serialize ONCE (so we don't double-dump massive payload)
            raw_json = json.dumps(data, ensure_ascii=False)

            # 3) Write raw locally
            local_file.write_text(raw_json, encoding="utf-8")
            payload_bytes = local_file.stat().st_size

            # 4) Push raw to HDFS
            t_hdfs = time.time()
            hdfs_mkdir(hdfs_dir)
            hdfs_put(str(local_file), hdfs_dir)
            hdfs_ms = int((time.time() - t_hdfs) * 1000)

            # 5) Send small summary to Kafka (ACK required)
            summary = {
                "source": "idfm",
                "feed": "next_departures_global",
                "ingest_time_utc": now.isoformat(),
                "payload_bytes": payload_bytes,
                "hdfs_path": hdfs_path,
                "url": "estimated-timetable?LineRef=ALL",
            }

            future = producer.send(KAFKA_TOPIC, summary)
            md = future.get(timeout=30)  # waits for broker ack or throws real error
            producer.flush(5)

            # ---- Influx metrics ----
            if influx_write:
                point = (
                    Point("pipeline_health")
                    .tag("source", "idfm")
                    .tag("feed", "next_departures_global")
                    .field("payload_bytes", payload_bytes)
                    .field("fetch_ms", fetch_ms)
                    .field("hdfs_ms", hdfs_ms)
                    .field("kafka_partition", md.partition)
                    .field("kafka_offset", md.offset)
                    .field("success", 1)
                    .time(now)
                )
                influx_write.write(
                    bucket=INFLUX_BUCKET,
                    org=INFLUX_ORG,
                    record=point
                )

            total_ms = int((time.time() - start_time) * 1000)

            print(
                f"[OK] raw→HDFS + summary→Kafka | "
                f"offset={md.offset} partition={md.partition} bytes={payload_bytes} fetch_ms={fetch_ms} "
                f"hdfs_ms={hdfs_ms}"
                f"total_ms={total_ms}"
            )

        except requests.HTTPError as e:
            # HTTP errors: 401, 403, 429, etc.
            status = getattr(e.response, "status_code", None)
            text = getattr(e.response, "text", "")
            print(f"[HTTP ERR] status={status} {e} body={text[:300]}")
        except subprocess.CalledProcessError as e:
            # HDFS put/mkdir failures
            print(f"[HDFS ERR] command failed: {e.cmd} returncode={e.returncode}")
        except Exception as e:
            print(f"[ERR] {type(e).__name__}: {e}")

        # sleep until next poll
        sleep_for = max(1, POLL_SECONDS - int(time.time() - start_time))
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
