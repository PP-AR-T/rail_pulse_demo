"""DataFrame logic shared by the simulator notebook, the declarative pipeline and local tests.

Every function takes and returns DataFrames and works on both batch and streaming inputs, so
the same code runs in a unit test, in a notebook and inside the pipeline.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .contracts import READING_FIELDS, SEVERITY_RANK

GROUP_COLS = [
    "pack",
    "site_id",
    "site_name",
    "asset_id",
    "asset_type",
    "sensor_id",
    "measure",
    "unit",
]

_CATALOGUE_SCHEMA = T.StructType(
    [
        T.StructField("pack", T.StringType()),
        T.StructField("site_id", T.StringType()),
        T.StructField("site_name", T.StringType()),
        T.StructField("asset_id", T.StringType()),
        T.StructField("asset_type", T.StringType()),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("measure", T.StringType()),
        T.StructField("unit", T.StringType()),
        T.StructField("period_s", T.IntegerType()),
        T.StructField("nominal", T.DoubleType()),
        T.StructField("noise", T.DoubleType()),
    ]
)

PLAN_SCHEMA = T.StructType(
    [
        T.StructField("pack", T.StringType()),
        T.StructField("scenario_id", T.StringType()),
        T.StructField("cause", T.StringType()),
        T.StructField("site_id", T.StringType()),
        T.StructField("asset_id", T.StringType()),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("measure", T.StringType()),
        T.StructField("kind", T.StringType()),
        T.StructField("per_min", T.DoubleType()),
        T.StructField("delta", T.DoubleType()),
        T.StructField("probability", T.DoubleType()),
        T.StructField("start_min", T.IntegerType()),
        T.StructField("end_min", T.IntegerType()),
    ]
)


def reading_schema() -> T.StructType:
    to_type = {"STRING": T.StringType(), "TIMESTAMP": T.TimestampType(), "DOUBLE": T.DoubleType()}
    return T.StructType([T.StructField(n, to_type[t]) for n, t in READING_FIELDS])


# ---- simulation ---------------------------------------------------------------------------------


def simulate_readings(
    spark: SparkSession,
    catalogue: list[dict],
    plan: list[dict],
    *,
    start_epoch_s: int,
    from_s: int,
    to_s: int,
    run_id: str,
    seed: int = 7,
) -> DataFrame:
    """Readings for every sensor between from_s and to_s seconds after the run start.

    Spark-native: the catalogue (thousands of rows) is exploded into readings on the executors,
    so volume scales with the cluster, not with the driver. Faults come from `plan`, with times
    in minutes after the run start.
    """
    cat = spark.createDataFrame(catalogue, _CATALOGUE_SCHEMA)
    faults = spark.createDataFrame(plan, PLAN_SCHEMA).select(
        "sensor_id", "kind", "per_min", "delta", "probability", "start_min", "end_min"
    )
    first = F.ceil(F.lit(from_s) / F.col("period_s")).cast("int") * F.col("period_s")
    offsets = F.when(
        first <= F.lit(to_s - 1), F.sequence(first, F.lit(to_s - 1), F.col("period_s"))
    ).otherwise(F.array().cast("array<int>"))

    df = (
        cat.withColumn("t", F.explode(offsets))
        .join(F.broadcast(faults), "sensor_id", "left")
        .withColumn("t_min", F.col("t") / 60.0)
        .withColumn(
            "active",
            F.col("kind").isNotNull()
            & (F.col("t_min") >= F.col("start_min"))
            & (F.col("t_min") < F.col("end_min")),
        )
        .filter(~(F.col("active") & (F.col("kind") == "dropout")))
        .withColumn("base", F.col("nominal") + F.col("noise") * F.randn(seed))
        .withColumn(
            "value",
            F.when(~F.col("active"), F.col("base"))
            .when(
                F.col("kind") == "drift",
                F.col("base") + F.col("per_min") * (F.col("t_min") - F.col("start_min")),
            )
            .when(F.col("kind") == "step", F.col("base") + F.col("delta"))
            .when(
                F.col("kind") == "spike",
                F.col("base")
                + F.when(F.rand(seed + 1) < F.col("probability"), F.col("delta")).otherwise(
                    F.lit(0.0)
                ),
            )
            .when(F.col("kind") == "flatline", F.col("nominal"))
            .otherwise(F.col("base")),
        )
        .withColumn("event_ts", F.timestamp_seconds(F.lit(start_epoch_s) + F.col("t")))
        .withColumn("quality", F.lit("good"))
        .withColumn("source", F.lit("synthetic"))
        .withColumn("run_id", F.lit(run_id))
    )
    return df.select(*[name for name, _ in READING_FIELDS])


# ---- detection ----------------------------------------------------------------------------------


def minute_aggregates(readings: DataFrame, watermark: str | None = "2 minutes") -> DataFrame:
    """1-minute tumbling windows per sensor. Works on a stream (with watermark) or a batch."""
    if watermark:
        readings = readings.withWatermark("event_ts", watermark)
    return (
        readings.groupBy(*GROUP_COLS, F.window("event_ts", "1 minute"))
        .agg(
            F.count("value").alias("n"),
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.stddev_pop("value").alias("std_value"),
        )
        .select(
            *GROUP_COLS,
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "n",
            "avg_value",
            "min_value",
            "max_value",
            "std_value",
        )
    )


def _detector_hits(packs: dict[str, dict]) -> Column:
    hits = []
    for pack_id, pack in packs.items():
        for d in pack["detectors"]:
            cond = (F.col("pack") == pack_id) & F.expr(f"({d['when']})")
            if d["measure"] != "*":
                cond = cond & (F.col("measure") == d["measure"])
            hits.append(
                F.when(
                    cond,
                    F.struct(
                        F.lit(d["id"]).alias("detector_id"),
                        F.lit(d["severity"]).alias("severity"),
                        F.lit(SEVERITY_RANK[d["severity"]]).alias("severity_rank"),
                    ),
                )
            )
    return F.filter(F.array(*hits), lambda h: h.isNotNull())


def detect(minutes: DataFrame, packs: dict[str, dict]) -> DataFrame:
    """Apply every pack's detector rules to 1-minute aggregates. Stateless: one row in, 0..n out."""
    return (
        minutes.withColumn("hit", F.explode(_detector_hits(packs))).select("*", "hit.*").drop("hit")
    )


def incidents(anomalies: DataFrame, gap: str = "10 minutes") -> DataFrame:
    """Collapse anomalies at one site that are close in time into one incident.

    This is the alarm-storm step: forty track-circuit alarms from one power supply become one row.
    """
    sw = F.session_window("window_start", gap)
    return (
        anomalies.groupBy("pack", "site_id", "site_name", sw.alias("sw"))
        .agg(
            F.count("*").alias("anomaly_count"),
            F.countDistinct("asset_id").alias("asset_count"),
            F.countDistinct("sensor_id").alias("sensor_count"),
            F.array_sort(F.collect_set("detector_id")).alias("detectors"),
            F.array_sort(F.collect_set("asset_type")).alias("asset_types"),
            F.max("severity_rank").alias("max_severity_rank"),
            F.min("window_start").alias("started_at"),
            F.max("window_end").alias("last_seen_at"),
            F.slice(F.array_sort(F.collect_set("asset_id")), 1, 10).alias("sample_assets"),
        )
        .withColumn(
            "incident_id",
            F.concat_ws("-", "pack", "site_id", F.date_format("sw.start", "yyyyMMdd'T'HHmm")),
        )
        .withColumn(
            "max_severity",
            F.element_at(
                F.create_map(*[x for k, v in SEVERITY_RANK.items() for x in (F.lit(v), F.lit(k))]),
                F.col("max_severity_rank"),
            ),
        )
        .drop("sw")
    )
