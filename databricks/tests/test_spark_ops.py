"""End-to-end tests of the shared Spark logic on a local session (skipped without PySpark).

They run the same functions the simulator and the pipeline use, as a stream, and check that
every planted fault becomes exactly one incident at the right site and nothing else does.
"""

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession  # noqa: E402

from accelerator import packs as P  # noqa: E402
from accelerator import spark_ops as S  # noqa: E402

START = 1_791_000_000 - 1_791_000_000 % 3600  # an exact hour, so minute windows line up


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="module")
def packs():
    return P.load_packs(ROOT / "domain_packs")


def run_stream(spark, tmp, pack, minutes=60, batch_minutes=15, scale=1):
    """Land batches like the simulator, then stream them through aggregates and detectors."""
    cat = P.sensor_catalogue(pack, scale)
    plan = P.fault_plan(pack, cat, 7)
    landing = tmp / "landing" / pack["pack"]
    for i, m0 in enumerate(range(0, minutes, batch_minutes)):
        S.simulate_readings(
            spark,
            cat,
            plan,
            start_epoch_s=START,
            from_s=m0 * 60,
            to_s=min(m0 + batch_minutes, minutes) * 60,
            run_id="test",
            seed=7 + i,
        ).write.mode("overwrite").parquet(str(landing / f"batch_{i:03d}"))

    readings = (
        spark.readStream.schema(S.reading_schema())
        .option("recursiveFileLookup", "true")
        .parquet(str(landing))
    )
    q = (
        S.detect(S.minute_aggregates(readings, "2 minutes"), {pack["pack"]: pack})
        .writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(tmp / "chk" / pack["pack"]))
        .trigger(availableNow=True)
        .start(str(tmp / "anomalies" / pack["pack"]))
    )
    q.awaitTermination()
    anomalies = spark.read.parquet(str(tmp / "anomalies" / pack["pack"]))
    return cat, plan, anomalies, S.incidents(anomalies, "10 minutes")


def scenario_sites(pack):
    return {s["id"]: s["target"]["sites"][0] for s in pack["scenarios"]}


@pytest.fixture(scope="module")
def results(spark, packs, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("stream")
    return {pid: run_stream(spark, tmp, packs[pid]) for pid in ("water", "rail")}


@pytest.mark.parametrize("pack_id", ["water", "rail"])
def test_each_planted_fault_becomes_one_incident_and_nothing_else_alarms(packs, results, pack_id):
    pack = packs[pack_id]
    rows = results[pack_id][3].collect()
    sites = sorted(r.site_id for r in rows)
    # Watermark holds back the last ~2 minutes; every scenario starts well before that.
    assert sites == sorted(scenario_sites(pack).values()), sites
    assert all(r.anomaly_count >= 1 for r in rows)


def test_alarm_storm_collapses_into_one_rail_incident(results):
    inc = results["rail"][3]
    wok = [r for r in inc.collect() if r.site_id == "WOK"]
    assert len(wok) == 1
    assert wok[0].asset_count == 40
    assert wok[0].anomaly_count >= 300  # 40 circuits x 8 minutes, plus edges
    assert "tc_low_voltage" in wok[0].detectors
    stuck = [r for r in inc.collect() if r.site_id == "BSK"][0]
    assert stuck.detectors == ["sensor_flatline"]  # a stuck sensor is flagged as such


def test_dropout_removes_readings(spark, packs):
    pack = packs["water"]
    cat = P.sensor_catalogue(pack, 1)
    plan = [{**p, "kind": "dropout"} for p in P.fault_plan(pack, cat, 7)]
    full = S.simulate_readings(
        spark, cat, [], start_epoch_s=START, from_s=0, to_s=3600, run_id="t"
    ).count()
    gaps = S.simulate_readings(
        spark, cat, plan, start_epoch_s=START, from_s=0, to_s=3600, run_id="t"
    ).count()
    assert gaps < full
