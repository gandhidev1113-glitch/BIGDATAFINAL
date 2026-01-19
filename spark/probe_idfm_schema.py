cat > /home/hadoop/pt_spark_jobs/probe_idfm_schema.py <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

spark = SparkSession.builder.appName("probe_idfm_schema").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

p = "hdfs://master:9000/data/pt/raw/source=idfm/dt=2026-01-16/hr=16/idfm_20260116T164327Z.json"
raw = spark.read.option("multiLine","false").json(p)

print("=== TOP LEVEL COLS ===")
print(raw.columns)

print("\n=== SCHEMA (top) ===")
raw.printSchema()

# check which branch exists WITHOUT show()ing the full objects
vmd_count = raw.selectExpr("Siri.ServiceDelivery.VehicleMonitoringDelivery").where(col("VehicleMonitoringDelivery").isNotNull()).count() if "Siri" in raw.columns else 0
smd_count = raw.selectExpr("Siri.ServiceDelivery.StopMonitoringDelivery").where(col("StopMonitoringDelivery").isNotNull()).count() if "Siri" in raw.columns else 0
etd_count = raw.selectExpr("Siri.ServiceDelivery.EstimatedTimetableDelivery").where(col("EstimatedTimetableDelivery").isNotNull()).count() if "Siri" in raw.columns else 0

print("\n=== BRANCH PRESENCE (non-null rows) ===")
print("VMD:", vmd_count, " SMD:", smd_count, " ETD:", etd_count)

# try VMD extraction (safe: take 5, select small strings only)
try:
    v = raw.selectExpr("explode(Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity) as va") \
          .selectExpr(
              "va.MonitoredVehicleJourney.LineRef.value as line",
              "va.MonitoredVehicleJourney.VehicleMode as mode",
              "va.MonitoredVehicleJourney.DatedVehicleJourneyRef.value as journey"
          ) \
          .limit(5)
    print("\n=== VMD sample (line/mode/journey) ===")
    v.show(truncate=False)
except Exception as e:
    print("\n[VMD probe failed]", e)

# try SMD extraction
try:
    s = raw.selectExpr("explode(Siri.ServiceDelivery.StopMonitoringDelivery.MonitoredStopVisit) as v") \
          .selectExpr(
              "v.MonitoredVehicleJourney.LineRef.value as line",
              "v.MonitoredVehicleJourney.VehicleMode as mode",
              "v.MonitoredVehicleJourney.DatedVehicleJourneyRef.value as journey"
          ) \
          .limit(5)
    print("\n=== SMD sample (line/mode/journey) ===")
    s.show(truncate=False)
except Exception as e:
    print("\n[SMD probe failed]", e)

spark.stop()
PY
