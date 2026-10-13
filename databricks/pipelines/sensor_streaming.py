# Pipeline source: sensor readings -> 1-minute aggregates -> pack detectors -> incidents.
#
# Domain-agnostic: this file never names a domain. It loads every pack in domain_packs/ and
# builds one flow per pack, so adding a domain is a new YAML file, not a code change.
# The DataFrame logic lives in accelerator/spark_ops.py and is unit-tested outside Databricks.
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

BUNDLE_ROOT = spark.conf.get("bundle_root")  # noqa: F821  (spark is provided by the pipeline)
LANDING = spark.conf.get("landing_path")  # noqa: F821
WATERMARK = spark.conf.get("watermark", "2 minutes")  # noqa: F821
INCIDENT_GAP = spark.conf.get("incident_gap", "10 minutes")  # noqa: F821
sys.path.insert(0, BUNDLE_ROOT)

from accelerator import packs as P  # noqa: E402
from accelerator import spark_ops as S  # noqa: E402

PACKS = P.load_packs(f"{BUNDLE_ROOT}/domain_packs")
PACK_LIST = ", ".join(f"'{p}'" for p in PACKS)


@dp.table(
    name="bronze_sensor_readings",
    comment="Sensor readings exactly as landed, one row per reading, append-only. Synthetic.",
)
@dp.expect_or_drop("has_event_time", "event_ts IS NOT NULL")
@dp.expect("value_is_a_number", "value IS NOT NULL AND NOT isnan(value)")
@dp.expect("known_pack", f"pack IN ({PACK_LIST})")
def bronze_sensor_readings():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "parquet")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .schema(S.reading_schema())
        .load(LANDING)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )


# One append flow per pack: each keeps its own watermark, so a busy domain cannot make
# another domain's readings look late.
dp.create_streaming_table(
    name="silver_sensor_minute",
    comment="1-minute tumbling aggregates per sensor (n, avg, min, max, std).",
)


def _minute_flow(pack_id: str):
    @dp.append_flow(target="silver_sensor_minute", name=f"minute_{pack_id}")
    def flow():
        readings = spark.readStream.table("bronze_sensor_readings").filter(  # noqa: F821
            F.col("pack") == pack_id
        )
        return S.minute_aggregates(readings, WATERMARK)


for _pack_id in PACKS:
    _minute_flow(_pack_id)


@dp.table(
    name="gold_anomalies",
    comment="One row per (sensor minute, detector) hit. Rules come from the domain packs.",
)
def gold_anomalies():
    return S.detect(spark.readStream.table("silver_sensor_minute"), PACKS)  # noqa: F821


@dp.materialized_view(
    name="gold_incidents",
    comment="Anomalies at one site within the incident gap collapsed into one incident.",
)
def gold_incidents():
    return S.incidents(spark.read.table("gold_anomalies"), INCIDENT_GAP)  # noqa: F821


@dp.materialized_view(
    name="gold_pack_summary",
    comment="Volume and freshness per pack: the numbers for the operator console header.",
)
def gold_pack_summary():
    readings = (
        spark.read.table("bronze_sensor_readings")  # noqa: F821
        .groupBy("pack")
        .agg(
            F.count("*").alias("readings"),
            F.countDistinct("sensor_id").alias("sensors"),
            F.max("event_ts").alias("latest_event_ts"),
            F.max("ingested_at").alias("latest_ingest_ts"),
        )
    )
    anomalies = (
        spark.read.table("gold_anomalies")  # noqa: F821
        .groupBy("pack")
        .agg(F.count("*").alias("anomalies"))
    )
    return readings.join(anomalies, "pack", "left").fillna({"anomalies": 0})
