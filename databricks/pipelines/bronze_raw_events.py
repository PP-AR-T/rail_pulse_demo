# Pipeline source: a streaming table fed by Auto Loader from the landing Volume.
# No writeStream, checkpoint or trigger here: the pipeline manages those.
# Table is named raw_events_dp so it does not clash with the notebook's bronze.raw_events.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENVELOPE_SCHEMA = (
    "fetched_at string, http_status int, rate_limits map<string,string>, error string, body string"
)


@dp.table(
    name="raw_events_dp",
    comment="Raw Realtime Trains envelope files for Bournemouth departures, append-only.",
)
@dp.expect("fetched_at_present", "fetched_at IS NOT NULL")
@dp.expect("status_or_error_present", "http_status IS NOT NULL OR error IS NOT NULL")
def raw_events_dp():
    files_df = (
        spark.readStream.format("cloudFiles")  # noqa: F821  (spark is provided by the pipeline)
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.json")
        .option("recursiveFileLookup", "true")
        .load(spark.conf.get("landing_path"))  # noqa: F821
    )
    return (
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
