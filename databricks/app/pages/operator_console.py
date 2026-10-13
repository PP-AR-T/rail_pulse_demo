"""Operator console: incident cards waiting for a human, and the decision log.

Reads and writes the ops schema through a SQL warehouse (app resource key: sql-warehouse).
Nothing here acts on equipment: a decision is a logged human judgement on an advisory card.
"""

import json
import os

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

CATALOG = os.environ.get("CATALOG", "rail_pulse")
SCHEMA = f"{CATALOG}.ops"
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
SEVERITY_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}

st.set_page_config(page_title="Operator console", layout="wide")
st.title("Operator console")
st.caption("Synthetic sensor data. A small model drafts each card; every decision is a human's.")

if not WAREHOUSE_ID:
    st.error("No SQL warehouse configured. Add a SQL warehouse resource with key `sql-warehouse`.")
    st.stop()


@st.cache_resource
def client() -> WorkspaceClient:
    return WorkspaceClient()


def query(sql: str, **params) -> pd.DataFrame:
    r = client().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
        parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()],
    )
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else r.status.state
        raise RuntimeError(f"query failed: {msg}")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest else []
    rows = (r.result.data_array or []) if r.result else []
    return pd.DataFrame(rows, columns=cols)


def operator() -> str:
    try:
        return st.context.headers.get("X-Forwarded-Email") or "operator"
    except Exception:
        return "operator"


try:
    summary = query(f"SELECT * FROM {SCHEMA}.gold_pack_summary ORDER BY pack")
    counts = query(f"""
        SELECT c.pack,
               count(*) AS cards,
               sum(CASE WHEN c.valid AND d.incident_id IS NULL THEN 1 ELSE 0 END) AS awaiting
        FROM {SCHEMA}.incident_cards c
        LEFT JOIN (SELECT DISTINCT incident_id FROM {SCHEMA}.operator_decisions) d
          ON d.incident_id = c.incident_id
        GROUP BY c.pack""")
except RuntimeError as e:
    st.warning(f"{e}. Run the rail_pulse_sensor_demo job first.")
    st.stop()

packs = summary["pack"].tolist()
pack = st.selectbox("Domain", packs, index=0) if packs else None
if pack is None:
    st.info("No readings yet. Run the rail_pulse_sensor_demo job.")
    st.stop()

s = summary[summary.pack == pack].iloc[0]
c = counts[counts.pack == pack]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Readings ingested", f"{int(s.readings):,}")
col2.metric("Sensors", f"{int(s.sensors):,}")
col3.metric("Anomalies detected", f"{int(s.anomalies):,}")
col4.metric("Cards awaiting you", int(c.awaiting.iloc[0]) if len(c) else 0)
st.caption(f"Latest reading {s.latest_event_ts} UTC · ingested {s.latest_ingest_ts} UTC")

st.subheader("Waiting for a decision")
cards = query(
    f"""
    SELECT c.* FROM {SCHEMA}.incident_cards c
    LEFT ANTI JOIN {SCHEMA}.operator_decisions d USING (incident_id)
    WHERE c.pack = :pack AND c.valid
    ORDER BY CASE c.max_severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
             c.created_at DESC
    LIMIT 20""",
    pack=pack,
)
if cards.empty:
    st.success("Nothing waiting.")

for _, card in cards.iterrows():
    icon = SEVERITY_ICON.get(card.max_severity, "⚪")
    with st.container(border=True):
        st.markdown(f"**{icon} {card.headline}**")
        st.caption(
            f"{card.site_name} · {card.started_at} to {card.last_seen_at} UTC · "
            f"{int(card.anomaly_count):,} alarms grouped · urgency {card.urgency} · "
            f"confidence {float(card.confidence):.0%} · model {card.model}"
        )
        st.write(card.what_happened)
        st.markdown(
            f"**Likely cause:** `{card.likely_cause}`  \n"
            f"**Impact:** {card.operational_impact}  \n"
            f"**Suggested action:** `{card.recommended_action}`  \n"
            f"**Evidence:** {', '.join(json.loads(card.evidence or '[]'))}"
        )
        reason = st.text_input("Reason (required)", key=f"reason-{card.incident_id}")
        b1, b2, b3 = st.columns(3)
        for col, decision in ((b1, "approve"), (b2, "dismiss"), (b3, "escalate")):
            if col.button(decision.capitalize(), key=f"{decision}-{card.incident_id}"):
                if not reason.strip():
                    st.warning("Add a reason first: the log is the audit trail.")
                else:
                    query(
                        f"""INSERT INTO {SCHEMA}.operator_decisions
                            VALUES (:incident_id, :decision, :reason, :who, current_timestamp())""",
                        incident_id=card.incident_id, decision=decision,
                        reason=reason.strip(), who=operator(),
                    )
                    st.toast(f"{decision.capitalize()}d {card.incident_id}")
                    st.rerun()

with st.expander("Cards the guardrail rejected"):
    st.dataframe(query(
        f"""SELECT incident_id, model, error, created_at FROM {SCHEMA}.incident_cards
            WHERE pack = :pack AND NOT valid ORDER BY created_at DESC LIMIT 50""",
        pack=pack,
    ), width="stretch")

st.subheader("Decision log")
st.dataframe(query(
    f"""SELECT d.decided_at, d.decided_by, d.decision, d.reason, c.headline, c.likely_cause,
               c.recommended_action, d.incident_id
        FROM {SCHEMA}.operator_decisions d JOIN {SCHEMA}.incident_cards c USING (incident_id)
        WHERE c.pack = :pack ORDER BY d.decided_at DESC LIMIT 100""",
    pack=pack,
), width="stretch")
