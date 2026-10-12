# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Auto Loader
# MAGIC Streams each new envelope file from the landing Volume into `bronze.raw_events`.
# MAGIC Append-only: nothing is edited. Auto Loader's checkpoint remembers which files are
# MAGIC already loaded, so re-running adds no duplicates.
# MAGIC
# MAGIC Data: Realtime Trains (realtimetrains.co.uk). Personal, non-commercial use.

# COMMAND ----------

dbutils.widgets.text("catalog", "rail_pulse")
dbutils.widgets.dropdown("trigger", "streaming", ["streaming", "run_once"])

CATALOG = dbutils.widgets.get("catalog")
TRIGGER = dbutils.widgets.get("trigger")
LANDING = f"/Volumes/{CATALOG}/bronze/landing/rtt/bmh"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/landing/_checkpoints/raw_events"  # outside LANDING
TABLE = f"{CATALOG}.bronze.raw_events"
print("landing   :", LANDING)
print("checkpoint:", CHECKPOINT)
print("table     :", TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Look first: the files the poller has landed

# COMMAND ----------

files = dbutils.fs.ls(LANDING)
print(len(files), "entries at the top level of the landing folder:")
for f in files:
    print(" ", f.path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stream the files into bronze
# MAGIC `binaryFile` hands us each file untouched; we keep the whole text as `raw_envelope`
# MAGIC and parse only the small fields we need next to it.

# COMMAND ----------

from pyspark.sql import functions as F

ENVELOPE_SCHEMA = (
    "fetched_at string, http_status int, rate_limits map<string,string>, error string, body string"
)

files_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.json")
    .option("recursiveFileLookup", "true")
    .load(LANDING)
)

bronze_df = (
    files_df.select(
        F.col("path").alias("source_file"),
        F.col("content").cast("string").alias("raw_envelope"),
    )
    .withColumn("e", F.from_json("raw_envelope", ENVELOPE_SCHEMA))
    .select(
        F.sha2("source_file", 256).alias("ingest_id"),
        "source_file",
        F.to_timestamp("e.fetched_at").alias("fetched_at"),
        F.col("e.http_status").alias("http_status"),
        F.col("e.rate_limits").alias("rate_limits"),
        F.col("e.error").alias("error"),
        F.col("e.body").alias("body"),
        "raw_envelope",
        F.current_timestamp().alias("ingested_at"),
    )
)

writer = bronze_df.writeStream.option("checkpointLocation", CHECKPOINT)

if TRIGGER == "streaming":
    try:
        query = writer.trigger(processingTime="30 seconds").toTable(TABLE)
        print("continuous stream started (checks for new files every 30 s)")
    except Exception as e:  # serverless may only allow run-once streams
        print("streaming trigger refused:", str(e)[:300])
        print("falling back to run-once; re-run this notebook every few minutes")
        query = writer.trigger(availableNow=True).toTable(TABLE)
        query.awaitTermination()
else:
    query = writer.trigger(availableNow=True).toTable(TABLE)
    query.awaitTermination()
    print("run-once finished")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check the result (run after a few minutes, and again after a re-run)

# COMMAND ----------

display(spark.sql(f"""
    SELECT count(*) AS rows,
           count(DISTINCT ingest_id) AS distinct_files,
           min(fetched_at) AS first_fetch,
           max(fetched_at) AS last_fetch,
           sum(CASE WHEN http_status = 200 THEN 1 ELSE 0 END) AS ok_200,
           sum(CASE WHEN http_status <> 200 OR http_status IS NULL THEN 1 ELSE 0 END) AS not_200
    FROM {TABLE}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT fetched_at, http_status, error, length(body) AS body_chars, source_file
    FROM {TABLE} ORDER BY fetched_at DESC LIMIT 10
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stop the stream (when you are done)

# COMMAND ----------

for q in spark.streams.active:
    print("stopping", q.id)
    q.stop()
