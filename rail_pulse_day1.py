# Databricks notebook source
# MAGIC %md
# MAGIC # Puente Rail Pulse — Live lane, day 1 on Databricks
# MAGIC
# MAGIC **One decision, end to end, on synthetic data.**
# MAGIC Train movements on the Bournemouth → London Waterloo corridor → 15-minute lateness windows → a cascade signal →
# MAGIC an agent proposes a passenger advisory with its evidence → a human approves, edits or rejects → an hour later the
# MAGIC decision is scored against what actually happened.
# MAGIC
# MAGIC *Data is synthetic and clearly labelled. Nothing here is production. The real feed (Network Rail open data) and the
# MAGIC Rust edge come next; this notebook is the Databricks variant of the lane, file-fed.*
# MAGIC
# MAGIC Runs on Databricks Free Edition (serverless). Also runs locally with `python -I rail_pulse_day1.py` using the stub model,
# MAGIC for pipeline testing only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

import json, math, random, re, uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import pandas as pd

IN_DATABRICKS = "spark" in globals()

CATALOG, SCHEMA = "rail_pulse", "demo"
DAY = "2026-10-12"                       # the synthetic day
HORIZON_MIN = 60                         # scoring horizon after a proposal
LATE_THRESHOLD_MIN = 10                  # a service is "late" above this
CASCADE_MIN_SERVICES = 2                 # late services in a window at one station to call a cascade
WINDOW_MIN = 15

# Model backend: "databricks" (Foundation Model APIs), "anthropic" (Claude API key in a widget/secret), or "stub" (tests only)
MODEL_BACKEND = "databricks" if IN_DATABRICKS else "stub"
DATABRICKS_MODEL = "databricks-meta-llama-3-3-70b-instruct"   # swap for databricks-claude-sonnet-4 if your workspace lists it
ANTHROPIC_MODEL = "claude-sonnet-4-5"

random.seed(7)

if IN_DATABRICKS:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"USE {CATALOG}.{SCHEMA}")

def save(df: pd.DataFrame, name: str):
    """Persist a pandas frame as a Delta table (Databricks) or print its shape (local)."""
    if IN_DATABRICKS:
        spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
        print(f"saved {CATALOG}.{SCHEMA}.{name}: {len(df)} rows")
    else:
        print(f"[local] {name}: {len(df)} rows")

def show(df: pd.DataFrame, n: int = 20):
    if IN_DATABRICKS:
        display(spark.createDataFrame(df.head(n)))
    else:
        print(df.head(n).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Synthetic replay — movements on the corridor (labelled synthetic)
# MAGIC
# MAGIC Eight stations, services every 30 minutes, a planted infrastructure incident at Southampton Central at 08:40 whose
# MAGIC delays cascade onto the next five services and partly recover downstream. Run times are approximate.

# COMMAND ----------

STATIONS = [  # (code, name, minutes from Bournemouth, approximate)
    ("BMH", "Bournemouth", 0), ("BCU", "Brockenhurst", 17), ("SOU", "Southampton Central", 32),
    ("WIN", "Winchester", 47), ("BSK", "Basingstoke", 65), ("WOK", "Woking", 85),
    ("CLJ", "Clapham Junction", 103), ("WAT", "London Waterloo", 112),
]
STATION_NAME = {c: n for c, n, _ in STATIONS}
INCIDENT = {"station": "SOU", "at": f"{DAY} 08:40", "kind": "points failure", "cascade": [8, 14, 22, 30, 35, 20, 12]}

def ts(hhmm: str) -> datetime:
    return datetime.fromisoformat(f"{DAY} {hhmm}")

def build_services():
    """Up services BMH→WAT departing every 20 min from 07:00 to 11:00."""
    services = []
    for i, dep in enumerate(range(7 * 60, 11 * 60 + 1, 20)):
        services.append({"service_id": f"1W{10 + i:02d}", "origin": "BMH", "dest": "WAT", "dep_min": dep})
    return services

def build_movements(services):
    rows, late_seq = [], 0
    incident_at = ts("08:40")
    for s in services:
        base_delay = random.choice([0, 0, 1, 2, 3])
        extra = 0
        for code, name, offset in STATIONS:
            planned = ts("00:00") + timedelta(minutes=s["dep_min"] + offset)
            # the incident catches any service passing SOU after it happened, for the next N services
            if code == "SOU" and planned >= incident_at and late_seq < len(INCIDENT["cascade"]):
                extra = INCIDENT["cascade"][late_seq]; late_seq += 1
            elif code in ("WIN", "BSK", "WOK", "CLJ", "WAT") and extra:
                extra = max(0, extra - random.choice([1, 2, 3]))   # partial recovery downstream
            actual = planned + timedelta(minutes=base_delay + extra + random.choice([-1, 0, 0, 1]))
            rows.append({
                "event_id": str(uuid.uuid4())[:8], "service_id": s["service_id"], "station": code,
                "station_name": name, "event_type": "ARRIVAL" if code == "WAT" else "DEPARTURE",
                "planned_ts": planned, "actual_ts": actual,
                "lateness_min": int(round((actual - planned).total_seconds() / 60)),
                "source": "synthetic", "schema_version": "0.1",
            })
    return pd.DataFrame(rows).sort_values("actual_ts").reset_index(drop=True)

services = build_services()
movements = build_movements(services)
timetable = pd.DataFrame([
    {"service_id": s["service_id"], "station": c, "planned_ts": ts("00:00") + timedelta(minutes=s["dep_min"] + o)}
    for s in services for c, _, o in STATIONS
])
save(movements, "movements"); save(timetable, "timetable")
show(movements[movements.station == "SOU"][["service_id", "station", "planned_ts", "actual_ts", "lateness_min"]], 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Signals — 15-minute lateness windows per station, cascade detection
# MAGIC
# MAGIC In the full lane this is the stream-processing step (Flink or the platform's managed equivalent). Here it is a
# MAGIC windowed aggregate over the replay, which is exactly what the streaming job computes.

# COMMAND ----------

def window_start(t: datetime) -> datetime:
    return t.replace(minute=(t.minute // WINDOW_MIN) * WINDOW_MIN, second=0, microsecond=0)

LOOKBACK_WINDOWS = 4   # a cascade = late services accumulating at one station over the last 60 minutes

def build_signals(mv: pd.DataFrame) -> pd.DataFrame:
    """Tumbling 15-minute windows per station, plus a 60-minute lookback so a cascade is a trend, not one late train."""
    mv = mv.copy(); mv["window_start"] = mv["actual_ts"].map(window_start)
    all_windows = pd.date_range(window_start(mv.actual_ts.min()), window_start(mv.actual_ts.max()), freq=f"{WINDOW_MIN}min")
    rows = []
    for station in [c for c, _, _ in STATIONS]:
        sm = mv[mv.station == station]
        history = []   # late services per window, most recent last
        for w in all_windows:
            inw = sm[sm.window_start == w]
            late = sorted(set(inw[inw.lateness_min > LATE_THRESHOLD_MIN].service_id))
            history.append(late)
            late_60 = sorted(set(s for h in history[-LOOKBACK_WINDOWS:] for s in h))
            rows.append({
                "station": station, "window_start": w, "window_end": w + timedelta(minutes=WINDOW_MIN),
                "services": sorted(set(inw.service_id)), "late_services": late,
                "mean_lateness": round(float(inw.lateness_min.mean()), 1) if len(inw) else 0.0,
                "max_lateness": int(inw.lateness_min.max()) if len(inw) else 0,
                "late_services_60min": late_60, "late_count": len(late_60),
                "cascade": bool(late) and len(late_60) >= CASCADE_MIN_SERVICES and int(inw.lateness_min.max()) >= LATE_THRESHOLD_MIN,
            })
    g = pd.DataFrame(rows).sort_values(["window_start", "station"]).reset_index(drop=True)
    g["signal_id"] = [f"sig-{i:03d}" for i in range(len(g))]
    return g

windows = build_signals(movements)
signals = windows[windows.cascade].reset_index(drop=True)
save(windows, "lateness_windows"); save(signals, "signals")
print(f"{len(signals)} cascade signals")
assert len(signals) >= 2, "no cascade detected — check the incident parameters"
show(signals[["signal_id", "station", "window_start", "late_count", "mean_lateness", "max_lateness", "late_services_60min"]], 10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lateness heatmap (station × window) — this is the picture for the post

# COMMAND ----------

heat = windows.pivot_table(index="station", columns="window_start", values="mean_lateness", aggfunc="mean").fillna(0)
heat = heat.reindex([c for c, _, _ in STATIONS])
heat.columns = [c.strftime("%H:%M") for c in heat.columns]
if IN_DATABRICKS:
    display(spark.createDataFrame(heat.reset_index()))   # switch the cell visualisation to Heatmap: x = window, y = station, value = lateness
else:
    print(heat.round(0).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The agent step — propose a passenger advisory with evidence
# MAGIC
# MAGIC Tools are plain functions over the tables (in the Agent Framework version they become Unity Catalog functions).
# MAGIC The model must answer with strict JSON. Invalid JSON is rejected and logged, never silently retried.

# COMMAND ----------

# ---- tools --------------------------------------------------------------------------------------------------
def tool_recent_signals(station: str, before: datetime, n: int = 4) -> list:
    s = windows[(windows.station == station) & (windows.window_start < before) & (windows.services.map(len) > 0)].tail(n)
    return s[["signal_id", "window_start", "late_count", "mean_lateness", "max_lateness", "late_services"]] \
        .assign(window_start=lambda d: d.window_start.dt.strftime("%H:%M")).to_dict("records")

def tool_upcoming_services(station: str, after: datetime, minutes: int = HORIZON_MIN) -> list:
    t = timetable[(timetable.station == station) & (timetable.planned_ts >= after) &
                  (timetable.planned_ts < after + timedelta(minutes=minutes))]
    return t.assign(planned_ts=lambda d: d.planned_ts.dt.strftime("%H:%M")).to_dict("records")

def tool_downstream_stations(station: str) -> list:
    codes = [c for c, _, _ in STATIONS]
    return [STATION_NAME[c] for c in codes[codes.index(station) + 1:]]

PROPOSAL_SCHEMA = {
    "advisory_text": str, "affected_services": list, "expected_delay_band": str,
    "likely_cause": str, "confidence": float, "evidence": list,
}
DELAY_BANDS = ["<15", "15-30", "30-60", ">60"]

SYSTEM_PROMPT = """You are the advisory-drafting step in a rail operations decision lane. You never publish anything;
a human controller approves, edits or rejects what you propose. Answer with ONE JSON object and nothing else, with exactly
these keys:
- advisory_text: a passenger-facing advisory, max 60 words, plain English, no speculation presented as fact
- affected_services: list of service_id strings you expect to be more than 10 minutes late in the next 60 minutes
- expected_delay_band: one of "<15", "15-30", "30-60", ">60" (typical delay for affected services)
- likely_cause: "unknown" unless the evidence states a cause
- confidence: number 0-1
- evidence: list of signal_id strings and tool names you relied on
Use only the evidence given. If evidence is thin, say so in the advisory and lower the confidence."""

def evidence_pack(sig: pd.Series) -> dict:
    return {
        "trigger_signal": {"signal_id": sig.signal_id, "station": STATION_NAME[sig.station], "station_code": sig.station,
                           "window": f"{sig.window_start:%H:%M}-{sig.window_end:%H:%M}", "late_count": int(sig.late_count),
                           "mean_lateness_min": float(sig.mean_lateness), "max_lateness_min": int(sig.max_lateness),
                           "late_services_this_window": list(sig.late_services),
                           "late_services_last_60_min": list(sig.late_services_60min)},
        "recent_signals": tool_recent_signals(sig.station, sig.window_start),
        "upcoming_services_next_60_min": tool_upcoming_services(sig.station, sig.window_end),
        "downstream_stations": tool_downstream_stations(sig.station),
        "incident_log": [],   # the real lane joins an incident feed here; today it is empty on purpose, so cause should be "unknown"
    }

# ---- model --------------------------------------------------------------------------------------------------
def call_model(system: str, user: str) -> str:
    if MODEL_BACKEND == "databricks":
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient().serving_endpoints.get_open_ai_client()
        r = client.chat.completions.create(model=DATABRICKS_MODEL, temperature=0.2, max_tokens=700,
                                           messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        return r.choices[0].message.content
    if MODEL_BACKEND == "anthropic":
        import anthropic
        key = dbutils.widgets.get("anthropic_api_key") if IN_DATABRICKS else None
        r = anthropic.Anthropic(api_key=key).messages.create(model=ANTHROPIC_MODEL, max_tokens=700, system=system,
                                                            messages=[{"role": "user", "content": user}])
        return r.content[0].text
    # stub: pipeline tests only — never present stub output as an agent's
    ev = json.loads(user.split("EVIDENCE:\n", 1)[1])
    trig = ev["trigger_signal"]
    nxt = [u["service_id"] for u in ev["upcoming_services_next_60_min"]][:3]
    return json.dumps({"advisory_text": f"[STUB] Delays of around {int(trig['mean_lateness_min'])} minutes through {trig['station']}. Cause under investigation.",
                       "affected_services": trig["late_services_last_60_min"] + nxt, "expected_delay_band": "15-30",
                       "likely_cause": "unknown", "confidence": 0.5, "evidence": [trig["signal_id"], "tool_upcoming_services"]})

def parse_proposal(raw: str):
    """Strict parse. Returns (proposal or None, error or None)."""
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return None, "no JSON object in response"
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return None, f"invalid JSON: {e}"
    for k, t in PROPOSAL_SCHEMA.items():
        if k not in obj:
            return None, f"missing key {k}"
        if t is float and isinstance(obj[k], int):
            obj[k] = float(obj[k])
        if not isinstance(obj[k], t):
            return None, f"key {k} should be {t.__name__}"
    if obj["expected_delay_band"] not in DELAY_BANDS:
        return None, f"expected_delay_band not in {DELAY_BANDS}"
    if not 0 <= obj["confidence"] <= 1:
        return None, "confidence out of range"
    return obj, None

if IN_DATABRICKS:
    # Which pay-per-token foundation models does this workspace expose? Pick one and set DATABRICKS_MODEL above.
    from databricks.sdk import WorkspaceClient
    names = [e.name for e in WorkspaceClient().serving_endpoints.list() if e.name.startswith("databricks-")]
    print("foundation model endpoints:", names)

try:
    import mlflow
    if MODEL_BACKEND == "databricks":
        mlflow.openai.autolog()      # traces every model call in the notebook's MLflow experiment
except Exception as e:
    print("mlflow tracing not enabled:", e)

def propose(sig: pd.Series) -> dict:
    ev = evidence_pack(sig)
    user = "Draft the advisory for this trigger. EVIDENCE:\n" + json.dumps(ev, default=str)
    raw = call_model(SYSTEM_PROMPT, user)
    obj, err = parse_proposal(raw)
    return {"proposal_id": f"prop-{sig.signal_id}", "signal_id": sig.signal_id, "station": sig.station,
            "proposed_at": sig.window_end, "model": {"databricks": DATABRICKS_MODEL, "anthropic": ANTHROPIC_MODEL, "stub": "stub"}[MODEL_BACKEND],
            "valid": err is None, "error": err or "", "raw": raw, **(obj or {})}

# Propose on the first two cascade signals (the first is thin evidence, the second has a trend — compare the scores)
proposals = pd.DataFrame([propose(signals.iloc[i]) for i in range(min(2, len(signals)))])
save(proposals.assign(evidence=lambda d: d.evidence.map(json.dumps), affected_services=lambda d: d.affected_services.map(json.dumps)), "proposals")
for _, p in proposals.iterrows():
    print(f"\n{p.proposal_id} valid={p.valid} error={p.error}")
    if p.valid:
        print(json.dumps({k: p[k] for k in PROPOSAL_SCHEMA}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The human gate — approve, edit or reject
# MAGIC
# MAGIC In the full lane this is a small approval service with an SLA timer. Here it is notebook widgets: set the three
# MAGIC widgets at the top of the notebook, then run this cell once per proposal. Every decision is logged with who, when and why.

# COMMAND ----------

if IN_DATABRICKS:
    dbutils.widgets.dropdown("proposal_id", proposals.proposal_id.iloc[0], list(proposals.proposal_id))
    dbutils.widgets.dropdown("decision", "approve", ["approve", "edit", "reject"])
    dbutils.widgets.text("reason", "")
    dbutils.widgets.dropdown("edited_delay_band", "(keep)", ["(keep)"] + DELAY_BANDS)
    dbutils.widgets.text("anthropic_api_key", "")

def record_decision(proposal_id: str, decision: str, reason: str, edited_delay_band: str = "(keep)", who: str = "controller") -> dict:
    p = proposals[proposals.proposal_id == proposal_id].iloc[0]
    if not p.valid:
        decision, reason = "reject", f"invalid proposal: {p.error}"
    band = p.expected_delay_band if edited_delay_band == "(keep)" else edited_delay_band
    return {"decision_id": f"dec-{proposal_id}", "proposal_id": proposal_id, "decision": decision, "reason": reason,
            "decided_by": who, "decided_at": p.proposed_at + timedelta(minutes=3), "final_delay_band": band,
            "final_affected_services": list(p.affected_services) if p.valid else [], "published": decision in ("approve", "edit")}

if IN_DATABRICKS:
    d = record_decision(dbutils.widgets.get("proposal_id"), dbutils.widgets.get("decision"),
                        dbutils.widgets.get("reason"), dbutils.widgets.get("edited_delay_band"))
    decisions = globals().get("decisions", pd.DataFrame())
    decisions = pd.concat([decisions[decisions.get("proposal_id", pd.Series(dtype=str)) != d["proposal_id"]] if len(decisions) else decisions,
                           pd.DataFrame([d])], ignore_index=True)
else:
    decisions = pd.DataFrame([record_decision(p, "approve", "evidence consistent with the windows") for p in proposals.proposal_id])
save(decisions.assign(final_affected_services=lambda x: x.final_affected_services.map(json.dumps)), "decisions")
show(decisions[["decision_id", "decision", "reason", "decided_by", "decided_at", "final_delay_band", "published"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Scoring — what actually happened in the next 60 minutes
# MAGIC
# MAGIC Precision and recall of the affected-services list, and whether the delay band contained the median actual lateness.
# MAGIC Bad scores stay on the board. The score is the product.

# COMMAND ----------

def band_contains(band: str, value: float) -> bool:
    lo, hi = {"<15": (0, 15), "15-30": (15, 30), "30-60": (30, 60), ">60": (60, 10**6)}[band]
    return lo <= value < hi

def score(dec: dict) -> dict:
    p = proposals[proposals.proposal_id == dec["proposal_id"]].iloc[0]
    start, end = p.proposed_at, p.proposed_at + timedelta(minutes=HORIZON_MIN)
    codes = [c for c, _, _ in STATIONS]
    downstream = codes[codes.index(p.station):]
    actual = movements[(movements.actual_ts >= start) & (movements.actual_ts < end) & movements.station.isin(downstream)]
    late = actual[actual.lateness_min > LATE_THRESHOLD_MIN]
    truth = set(late.service_id)
    pred = set(dec["final_affected_services"])
    tp = len(truth & pred)
    precision = tp / len(pred) if pred else 0.0
    recall = tp / len(truth) if truth else (1.0 if not pred else 0.0)
    median_late = float(late.groupby("service_id").lateness_min.max().median()) if len(late) else 0.0
    return {"decision_id": dec["decision_id"], "proposal_id": dec["proposal_id"], "published": dec["published"],
            "horizon_end": end, "actual_late_services": sorted(truth), "predicted_services": sorted(pred),
            "precision": round(precision, 2), "recall": round(recall, 2),
            "median_actual_lateness": median_late, "band": dec["final_delay_band"],
            "band_hit": band_contains(dec["final_delay_band"], median_late) if dec["published"] else False}

scores = pd.DataFrame([score(d) for d in decisions.to_dict("records")])
save(scores.assign(actual_late_services=lambda x: x.actual_late_services.map(json.dumps), predicted_services=lambda x: x.predicted_services.map(json.dumps)), "scores")
show(scores[["decision_id", "published", "precision", "recall", "band", "median_actual_lateness", "band_hit"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The decision log, end to end (one row per decision — the screenshot for the post)

# COMMAND ----------

log = (proposals[["proposal_id", "signal_id", "station", "proposed_at", "model", "valid", "expected_delay_band", "confidence", "advisory_text"]]
       .merge(decisions[["proposal_id", "decision", "reason", "decided_by", "final_delay_band"]], on="proposal_id", how="left")
       .merge(scores[["proposal_id", "precision", "recall", "band_hit"]], on="proposal_id", how="left"))
log["station"] = log.station.map(STATION_NAME)
save(log, "decision_log")
show(log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's real and what isn't
# MAGIC
# MAGIC | Real | Not yet |
# MAGIC |---|---|
# MAGIC | The windowing, the strict-JSON agent step, the human gate, the scorer, the decision log | Live Network Rail feed (account requested this week) |
# MAGIC | Delta tables in Unity Catalog; MLflow traces of every model call | Rust edge and Kafka/Redpanda — Free Edition is serverless and can't reach a local broker, so this variant is file-fed |
# MAGIC | A wrong proposal, scored and kept | Approval service with SLA timer (today: widgets) |
# MAGIC
# MAGIC Next: Unity Catalog functions as tools + Mosaic AI Agent Framework for the agent step; Agent Evaluation over the decision log; the real replay file from M0.
