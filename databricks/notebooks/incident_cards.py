# Databricks notebook source
# MAGIC %md
# MAGIC # Incident cards: the agent step
# MAGIC For each new incident from the streaming pipeline, a small model drafts one incident
# MAGIC card for the operator: what happened, likely cause, impact, recommended action, urgency
# MAGIC and evidence.
# MAGIC
# MAGIC - The model never sees raw readings. It gets a compact evidence pack (one line per
# MAGIC   detector), the domain pack's knowledge notes and a closed list of causes and actions.
# MAGIC - Every card is validated against the contract before anyone sees it. Rejected cards
# MAGIC   are kept with the reason, so the guardrail is visible.
# MAGIC - Cards wait for a human in the operator console. Nothing is actioned automatically.
# MAGIC - The last section scores cards against the simulator's ground truth.

# COMMAND ----------

import json
import os
import sys
from datetime import UTC, datetime

dbutils.widgets.text("catalog", "rail_pulse")
dbutils.widgets.text("model", "")  # blank: smallest model this workspace serves
dbutils.widgets.dropdown("backend", "databricks", ["databricks", "stub"])
dbutils.widgets.text("max_cards", "6")

CATALOG = dbutils.widgets.get("catalog")
BACKEND = dbutils.widgets.get("backend")
MAX_CARDS = int(dbutils.widgets.get("max_cards"))
SCHEMA = f"{CATALOG}.ops"

BUNDLE_ROOT = os.path.abspath("..")
sys.path.insert(0, BUNDLE_ROOT)

from accelerator import agent as A  # noqa: E402
from accelerator import packs as P  # noqa: E402

PACKS = P.load_packs(f"{BUNDLE_ROOT}/domain_packs")
print("packs:", list(PACKS))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pick the model (small by default)

# COMMAND ----------

if BACKEND == "databricks":
    from databricks.sdk import WorkspaceClient

    _w = WorkspaceClient()
    served = [e.name for e in _w.serving_endpoints.list() if e.name.startswith("databricks-")]
    MODEL = A.pick_model(served, dbutils.widgets.get("model"))
    _client = _w.serving_endpoints.get_open_ai_client()

    def call_model(system: str, user: str) -> str:
        r = _client.chat.completions.create(
            model=MODEL,
            temperature=0,
            max_tokens=600,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        return r.choices[0].message.content

    try:
        import mlflow

        mlflow.openai.autolog()  # every call traced in this notebook's MLflow experiment
    except Exception as e:
        print("MLflow tracing not enabled:", e)
else:
    MODEL, call_model = "stub", A.stub_model

print("model:", MODEL)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Incidents without a card yet

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.incident_cards (
      incident_id STRING, pack STRING, site_id STRING, site_name STRING, model STRING,
      valid BOOLEAN, error STRING, latency_ms INT, prompt_chars INT,
      headline STRING, what_happened STRING, likely_cause STRING, operational_impact STRING,
      recommended_action STRING, urgency STRING, confidence DOUBLE, evidence STRING,
      anomaly_count BIGINT, max_severity STRING, started_at TIMESTAMP, last_seen_at TIMESTAMP,
      evidence_json STRING, raw STRING, created_at TIMESTAMP)
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.operator_decisions (
      incident_id STRING, decision STRING, reason STRING, decided_by STRING, decided_at TIMESTAMP)
""")

todo = spark.sql(f"""
    SELECT i.* FROM {SCHEMA}.gold_incidents i
    LEFT ANTI JOIN {SCHEMA}.incident_cards c USING (incident_id)
    WHERE i.pack IN ({", ".join(f"'{p}'" for p in PACKS)})
    ORDER BY i.max_severity_rank DESC, i.anomaly_count DESC
    LIMIT {MAX_CARDS}
""").collect()
print(f"{len(todo)} incidents to card (max_cards={MAX_CARDS})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Draft, validate, store

# COMMAND ----------

cards = []
for row in todo:
    inc = row.asDict()
    anomalies = [
        r.asDict()
        for r in spark.sql(f"""
        SELECT * FROM {SCHEMA}.gold_anomalies
        WHERE pack = '{inc["pack"]}' AND site_id = '{inc["site_id"]}'
          AND window_start >= TIMESTAMP '{inc["started_at"]}'
          AND window_end <= TIMESTAMP '{inc["last_seen_at"]}'
    """).collect()
    ]
    card = A.draft_card(inc, anomalies, PACKS[inc["pack"]], call_model)
    card.update(
        model=MODEL,
        site_name=inc["site_name"],
        anomaly_count=inc["anomaly_count"],
        max_severity=inc["max_severity"],
        started_at=inc["started_at"],
        last_seen_at=inc["last_seen_at"],
        created_at=datetime.now(UTC),
        evidence=json.dumps(card.get("evidence", [])),
    )
    cards.append(card)
    flag = "valid" if card["valid"] else f"REJECTED: {card['error']}"
    print(f"{inc['incident_id']}: {card.get('headline', '-')}  [{flag}, {card['latency_ms']} ms]")

if cards:
    cols = [f.name for f in spark.table(f"{SCHEMA}.incident_cards").schema]
    rows = [{c: card.get(c) for c in cols} for card in cards]
    spark.createDataFrame(rows, spark.table(f"{SCHEMA}.incident_cards").schema).write.mode(
        "append"
    ).saveAsTable(f"{SCHEMA}.incident_cards")

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT incident_id, valid, max_severity, anomaly_count, headline, likely_cause,
           recommended_action, urgency, confidence, error
    FROM {SCHEMA}.incident_cards ORDER BY created_at DESC LIMIT 20
""")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Alarm compression: how many alarms an operator no longer has to read one by one

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT a.pack, a.anomalies, i.incidents,
           round(a.anomalies / i.incidents, 1) AS alarms_per_incident
    FROM (SELECT pack, count(*) AS anomalies FROM {SCHEMA}.gold_anomalies GROUP BY pack) a
    JOIN (SELECT pack, count(*) AS incidents FROM {SCHEMA}.gold_incidents GROUP BY pack) i
      USING (pack)
""")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Score against ground truth (synthetic data only: real data has no answer key)

# COMMAND ----------

display(
    spark.sql(f"""
    WITH truth AS (
      SELECT c.incident_id, array_sort(collect_set(m.cause)) AS true_causes
      FROM {SCHEMA}.incident_cards c
      JOIN {SCHEMA}.sim_fault_manifest m
        ON m.pack = c.pack AND m.site_id = c.site_id
       AND m.fault_start_ts <= c.last_seen_at AND m.fault_end_ts >= c.started_at
      GROUP BY c.incident_id
    )
    SELECT c.incident_id, c.model, c.valid, c.likely_cause, t.true_causes,
           array_contains(t.true_causes, c.likely_cause) AS cause_correct, c.confidence
    FROM {SCHEMA}.incident_cards c LEFT JOIN truth t USING (incident_id)
    ORDER BY c.created_at DESC
""")
)
