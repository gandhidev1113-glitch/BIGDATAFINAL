1. Project Overview

This project implements an end-to-end Big Data analytics pipeline for public transport data (IDFM – Île-de-France Mobilités).

The system ingests raw transport data, processes it at scale using Apache Spark on YARN, stores analytical metrics in InfluxDB, and visualizes them using Grafana dashboards.

The objective is to demonstrate:
	•	Distributed storage (HDFS)
	•	Resource management (YARN)
	•	Streaming & messaging (Kafka)
	•	Batch analytics (Spark)
	•	Time-series analytics (InfluxDB)
	•	Observability & monitoring (Grafana)

2. Architecture Overview

Pipeline flow:

IDFM API / Files
        ↓
Kafka (Topics)
        ↓
HDFS (Raw Zone)
        ↓
Spark on YARN
        ↓
InfluxDB (Time-series metrics)
        ↓
Grafana Dashboards

3. Technologies Used

Component
Purpose
HDFS
Distributed storage for raw data
YARN
Resource manager for Spark jobs
Kafka
Message broker for ingestion
Spark
Batch analytics & aggregation
InfluxDB
Time-series metrics storage
Grafana
Visualization & dashboards
Python
Ingestion & Spark jobs

4. Directory Structure

/data/pt/
├── raw/
│   └── source=idfm/
│       └── dt=YYYY-MM-DD/hr=HH/
│
/home/hadoop/
├── pt_spark_jobs/
│   ├── pt_hourly_metrics.py
│   ├── pt_line_hourly_metrics.py
│
├── idfm_ingest/
│   └── idfm_to_kafka.py
│
├── spark-analytics-env/
│
/usr/local/
├── kafka/
├── hadoop/
├── spark/


5. Environment & Prerequisites

Cluster
	•	1 master node
	•	3+ worker nodes
	•	Hadoop + YARN running
	•	Kafka running on master
	•	InfluxDB running on master
	•	Grafana running on master

Python
	•	Python 3.12
	•	Virtual environment: spark-analytics-env

6. End-to-End Data Flow
	1.	Data ingestion
	•	Data ingested via Kafka producer (idfm_to_kafka.py)
	2.	Raw storage
	•	Kafka consumer writes JSON files to HDFS
	3.	Batch analytics
	•	Spark jobs aggregate hourly metrics
	4.	Metrics storage
	•	Results written to InfluxDB
	5.	Visualization
	•	Grafana dashboards query InfluxDB

7. Setup & Installation

Start core services (Master)

# HDFS
start-dfs.sh

# YARN
start-yarn.sh

# Kafka
systemctl start kafka

m6-# InfluxDB
systemctl start influxdb

# Grafana
systemctl start grafana-server

Verify:
jps
yarn node -list
hdfs dfsadmin -report

8. Running the Pipeline (Step-by-Step)

Step 1 – Kafka topic
/usr/local/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.0.0.46:9092 \
  --create \
  --topic idfm_raw \
  --partitions 3 \
  --replication-factor 1

Step 2 – Ingest data to Kafka
source ~/idfm-ingest-env/bin/activate
python idfm_to_kafka.py

Step 3 – Run Spark hourly metrics job
source ~/spark-analytics-env/bin/activate

spark-submit \
  --master yarn \
  --deploy-mode client \
  /home/hadoop/pt_spark_jobs/pt_hourly_metrics.py

Writes to InfluxDB:
	•	Measurement: pt_analytics_hourly

Step 4 – Run Spark line-level metrics job
spark-submit \
  --master yarn \
  --deploy-mode client \
  /home/hadoop/pt_spark_jobs/pt_line_hourly_metrics.py

Writes to InfluxDB:
	•	Measurement: pt_line_hourly

9. Grafana Dashboard

Data source
	•	Type: InfluxDB v2
	•	Bucket: public_transport
	•	Org: esilv

Panels
	•	Files processed per hour
	•	Total bytes per hour
	•	Avg delay (seconds)
	•	P95 delay
	•	Top lines by volume
	•	Mode activity (bus, metro, tram, etc.)

10. Verification
# HDFS
hdfs dfs -ls /data/pt/raw/

# YARN
yarn application -list

# Kafka
/usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.0.0.46:9092

# InfluxDB
influx bucket list

11. Troubleshooting Runbook

Kafka: NotLeaderForPartitionError

Cause: wrong advertised.listeners or replication factor > brokers
Fix:
kafka-topics.sh --delete --topic idfm_raw
kafka-topics.sh --create --topic idfm_raw --replication-factor 1

Spark executor lost / timeout

Cause: memory pressure or network timeout
Fix:
	•	Reduce executors
	•	Increase spark.network.timeout
	•	Restart YARN NodeManagers

HDFS datanode not joining

Cause: BlockPool ID mismatch
Fix:
rm -rf /usr/local/hadoop/data/datanode/*
hdfs --daemon start datanode

12. Limitations & Future Improvements
	•	Add real-time Spark Structured Streaming
	•	Add alerting in Grafana
	•	Improve Kafka replication
	•	Optimize Spark partitioning
	•	Add schema validation

