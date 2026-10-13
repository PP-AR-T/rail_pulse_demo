# Databricks notebook source
# MAGIC %md
# MAGIC # Sensor simulator (synthetic, labelled)
# MAGIC Generates sensor readings for any domain pack and lands them as Parquet micro-batches in
# MAGIC the landing Volume, where the streaming pipeline picks them up with Auto Loader.
# MAGIC
# MAGIC - **Volume:** raise `scale` to multiply the number of assets per site. The rail pack at
# MAGIC   scale 1 is about 2.4 million readings per simulated hour; scale 10 is about 24 million.
# MAGIC - **Faults:** the pack's scenarios are replayed in every run and written to
# MAGIC   `sim_fault_manifest` as ground truth, used only to score the agent afterwards.
# MAGIC - **Clock:** each run continues from where the previous run of the same pack ended, so the
# MAGIC   stream never receives data older than it has already seen.
# MAGIC - **Pace:** `pause_s = 0` writes every batch at once (replay). `pause_s > 0` drips one
# MAGIC   batch at a time, for a live-looking demo while the pipeline runs.
# MAGIC
# MAGIC Every reading carries `source = 'synthetic'`. Nothing here is real asset data.

# COMMAND ----------

import os
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

dbutils.widgets.text("catalog", "rail_pulse")
dbutils.widgets.text("pack", "rail")
dbutils.widgets.text("scale", "1")
dbutils.widgets.text("minutes", "60")
dbutils.widgets.text("batch_minutes", "5")
dbutils.widgets.text("pause_s", "0")
dbutils.widgets.text("files_per_batch", "4")
dbutils.widgets.text("seed", "7")

CATALOG = dbutils.widgets.get("catalog")
PACK = dbutils.widgets.get("pack")
SCALE = int(dbutils.widgets.get("scale"))
MINUTES = int(dbutils.widgets.get("minutes"))
BATCH_MIN = int(dbutils.widgets.get("batch_minutes"))
PAUSE_S = float(dbutils.widgets.get("pause_s"))
FILES = int(dbutils.widgets.get("files_per_batch"))
SEED = int(dbutils.widgets.get("seed"))

BUNDLE_ROOT = os.path.abspath("..")  # notebooks/ sits next to accelerator/ and domain_packs/
sys.path.insert(0, BUNDLE_ROOT)

from accelerator import packs as P  # noqa: E402
from accelerator.spark_ops import simulate_readings  # noqa: E402

SCHEMA = f"{CATALOG}.ops"
LANDING = f"/Volumes/{CATALOG}/bronze/landing/sensors"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

# COMMAND ----------
# MAGIC %md
# MAGIC ## The pack, expanded into sensors and planted faults

# COMMAND ----------

pack = P.load_pack(f"{BUNDLE_ROOT}/domain_packs/{PACK}.yml")
catalogue = P.sensor_catalogue(pack, SCALE)
plan = P.fault_plan(pack, catalogue, SEED)
per_hour = P.readings_per_hour(catalogue)

print(
    f"pack {PACK}: {len(pack['sites'])} sites, {len({c['asset_id'] for c in catalogue}):,} assets, "
    f"{len(catalogue):,} sensors"
)
print(
    f"about {per_hour:,} readings per simulated hour; this run: about {per_hour * MINUTES // 60:,}"
)
print(f"{len({p['scenario_id'] for p in plan})} fault scenarios on {len(plan)} sensors")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Where this run starts on the clock

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.sim_runs (
      run_id STRING, pack STRING, scale INT, sensors INT, start_ts TIMESTAMP, end_ts TIMESTAMP,
      batches INT, rows BIGINT, seconds_to_generate DOUBLE, landed_at TIMESTAMP)
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.sim_fault_manifest (
      run_id STRING, pack STRING, scenario_id STRING, cause STRING, site_id STRING,
      asset_id STRING, sensor_id STRING, measure STRING, kind STRING,
      fault_start_ts TIMESTAMP, fault_end_ts TIMESTAMP)
""")

now = datetime.now(UTC).replace(second=0, microsecond=0)
prev_end = spark.sql(
    f"SELECT max(end_ts) AS e FROM {SCHEMA}.sim_runs WHERE pack = '{PACK}'"
).first()["e"]
start = now - timedelta(minutes=MINUTES)
if prev_end is not None:
    start = max(start, prev_end.replace(tzinfo=UTC))
end = start + timedelta(minutes=MINUTES)
run_id = f"{PACK}-{start:%Y%m%dT%H%M}-{uuid.uuid4().hex[:6]}"
print(f"run {run_id}: simulated {start:%Y-%m-%d %H:%M} to {end:%H:%M} UTC")

manifest = spark.createDataFrame(
    [
        (
            run_id,
            p["pack"],
            p["scenario_id"],
            p["cause"],
            p["site_id"],
            p["asset_id"],
            p["sensor_id"],
            p["measure"],
            p["kind"],
            start + timedelta(minutes=p["start_min"]),
            start + timedelta(minutes=min(p["end_min"], MINUTES)),
        )
        for p in plan
        if p["start_min"] < MINUTES
    ],
    "run_id string, pack string, scenario_id string, cause string, site_id string, "
    "asset_id string, sensor_id string, measure string, kind string, "
    "fault_start_ts timestamp, fault_end_ts timestamp",
)
manifest.write.mode("append").saveAsTable(f"{SCHEMA}.sim_fault_manifest")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate and land the micro-batches

# COMMAND ----------

run_dir = f"{LANDING}/{PACK}/{run_id}"
batches = range(0, MINUTES, BATCH_MIN)
t0 = time.perf_counter()
for i, m0 in enumerate(batches):
    m1 = min(m0 + BATCH_MIN, MINUTES)
    df = simulate_readings(
        spark,
        catalogue,
        plan,
        start_epoch_s=int(start.timestamp()),
        from_s=m0 * 60,
        to_s=m1 * 60,
        run_id=run_id,
        seed=SEED + i,
    )
    df.repartition(FILES).write.mode("overwrite").parquet(f"{run_dir}/batch_{i:03d}")
    print(f"batch {i:03d}: minutes {m0}-{m1} landed")
    if PAUSE_S and i < len(batches) - 1:
        time.sleep(PAUSE_S)
gen_s = time.perf_counter() - t0

rows = spark.read.parquet(f"{run_dir}/*").count()
spark.createDataFrame(
    [
        (
            run_id,
            PACK,
            SCALE,
            len(catalogue),
            start,
            end,
            len(batches),
            rows,
            gen_s,
            datetime.now(UTC),
        )
    ],
    "run_id string, pack string, scale int, sensors int, start_ts timestamp, end_ts timestamp, "
    "batches int, rows bigint, seconds_to_generate double, landed_at timestamp",
).write.mode("append").saveAsTable(f"{SCHEMA}.sim_runs")

print(
    f"{rows:,} readings in {len(batches)} batches, generated in {gen_s:,.0f} s "
    f"({rows / max(gen_s, 1e-9):,.0f} readings/s, including pauses)"
)
print("landed under", run_dir)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {SCHEMA}.sim_runs ORDER BY landed_at DESC LIMIT 10"))

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT scenario_id, cause, site_id, count(*) AS sensors, min(fault_start_ts) AS starts,
           max(fault_end_ts) AS ends
    FROM {SCHEMA}.sim_fault_manifest WHERE run_id = '{run_id}'
    GROUP BY ALL ORDER BY starts
""")
)
