"""The agent step: one incident in, one validated incident card out. Pure Python.

The model is deliberately small. It does not read raw sensor data: detection has already been
done deterministically. It receives a compact, pre-digested evidence pack (one line per detector,
not thousands of readings), the pack's own knowledge notes, and a closed list of causes and
actions. Its job is explanation and triage, which small models do well when the context is tight.
"""

import json
import time
from collections import defaultdict

from .contracts import HEADLINE_MAX, UNKNOWN_CAUSE, URGENCY, card_json_schema, parse_card
from .packs import detector_index, knowledge_for

# Smallest first. The notebook picks the first one this workspace actually serves.
SMALL_MODEL_PREFERENCE = [
    "databricks-meta-llama-3-1-8b-instruct",
    "databricks-gemma-3-12b",
    "databricks-llama-4-maverick",
    "databricks-meta-llama-3-3-70b-instruct",
]


def pick_model(available: list[str], preferred: str = "", fallback=SMALL_MODEL_PREFERENCE) -> str:
    if preferred and preferred in available:
        return preferred
    for name in fallback:
        if name in available:
            return name
    raise ValueError(f"none of {fallback} is served here; pick one of {sorted(available)}")


def evidence_pack(
    incident: dict, anomalies: list[dict], pack: dict, max_detectors: int = 12
) -> dict:
    """Summarise an incident's anomalies by detector, the way an engineer would read them."""
    dets = detector_index(pack)
    by_det = defaultdict(list)
    for a in anomalies:
        by_det[a["detector_id"]].append(a)

    lines = []
    for det_id, rows in by_det.items():
        d = dets[det_id]
        worst_col = (
            "min_value"
            if any(c in d["when"] for c in ("min_value <", "avg_value <"))
            else "max_value"
        )
        worst = (min if worst_col == "min_value" else max)(r[worst_col] for r in rows)
        lines.append(
            {
                "detector_id": det_id,
                "meaning": d["meaning"],
                "severity": d["severity"],
                "measure": rows[0]["measure"],
                "unit": rows[0]["unit"],
                "anomaly_minutes": len(rows),
                "assets_affected": len({r["asset_id"] for r in rows}),
                "example_assets": sorted({r["asset_id"] for r in rows})[:5],
                "worst_value": round(float(worst), 3),
                "first_seen": str(min(r["window_start"] for r in rows)),
                "last_seen": str(max(r["window_end"] for r in rows)),
            }
        )
    lines.sort(
        key=lambda x: (
            -{"info": 1, "warning": 2, "critical": 3}[x["severity"]],
            -x["anomaly_minutes"],
        )
    )
    lines = lines[:max_detectors]

    evidence_ids = sorted(
        {x["detector_id"] for x in lines} | {a for x in lines for a in x["example_assets"]}
    )
    return {
        "pack": pack["pack"],
        "site": {"id": incident["site_id"], "name": incident["site_name"]},
        "incident": {
            "incident_id": incident["incident_id"],
            "started_at": str(incident["started_at"]),
            "last_seen_at": str(incident["last_seen_at"]),
            "anomaly_count": int(incident["anomaly_count"]),
            "asset_count": int(incident["asset_count"]),
            "asset_types": list(incident["asset_types"]),
        },
        "signals_by_detector": lines,
        "knowledge": knowledge_for(pack, [x["detector_id"] for x in lines]),
        "allowed_causes": [{"id": c["id"], "label": c["label"]} for c in pack["causes"]],
        "allowed_actions": [{"id": a["id"], "label": a["label"]} for a in pack["actions"]],
        "evidence_ids": evidence_ids,
    }


def system_prompt(pack: dict) -> str:
    return (
        f"{pack['agent']['role'].strip()}\n\n"
        "Rules:\n"
        "- Use only the EVIDENCE given. Do not invent readings, assets or events.\n"
        f'- likely_cause must be one id from allowed_causes, or "{UNKNOWN_CAUSE}" if the evidence '
        "does not support one.\n"
        "- recommended_action must be one id from allowed_actions. You advise; a human decides. "
        "Never instruct anyone to operate safety-critical equipment.\n"
        "- evidence must list ids copied exactly from evidence_ids that support your cause.\n"
        f"- urgency is one of {list(URGENCY)}. headline is at most {HEADLINE_MAX} characters.\n"
        "- Many alarms at one site at the same moment usually share one cause; say so.\n"
        "- A sensor that stops changing is a sensor fault, not an asset fault.\n"
        "Reply with one JSON object only, with exactly these keys: headline, what_happened, "
        "likely_cause, operational_impact, recommended_action, urgency, confidence, evidence. "
        "Begin your answer with {"
    )


def user_prompt(evidence: dict) -> str:
    return "Write the incident card. EVIDENCE:\n" + json.dumps(evidence, default=str)


def stub_model(system: str, user: str) -> str:
    """Deterministic stand-in for tests. Never present its output as an agent's."""
    ev = json.loads(user.split("EVIDENCE:\n", 1)[1])
    top = ev["signals_by_detector"][0]
    return json.dumps(
        {
            "headline": f"[STUB] {top['detector_id']} at {ev['site']['name']}"[:HEADLINE_MAX],
            "what_happened": f"[STUB] {ev['incident']['anomaly_count']} anomalies on "
            f"{ev['incident']['asset_count']} assets.",
            "likely_cause": UNKNOWN_CAUSE,
            "operational_impact": "[STUB] unknown",
            "recommended_action": ev["allowed_actions"][0]["id"],
            "urgency": "soon",
            "confidence": 0.3,
            "evidence": [top["detector_id"]],
        }
    )


def draft_card(incident: dict, anomalies: list[dict], pack: dict, call_model) -> dict:
    """Run the agent for one incident. Always returns a row, valid or not, for the audit trail."""
    ev = evidence_pack(incident, anomalies, pack)
    t0 = time.perf_counter()
    try:
        raw = call_model(system_prompt(pack), user_prompt(ev))
        err_call = None
    except Exception as e:  # keep the row: a failed call is part of the record
        raw, err_call = "", f"model call failed: {e!r}"[:500]
    latency_ms = int((time.perf_counter() - t0) * 1000)
    card, err = (
        (None, err_call)
        if err_call
        else parse_card(
            raw,
            cause_ids=[c["id"] for c in pack["causes"]],
            action_ids=[a["id"] for a in pack["actions"]],
            evidence_ids=ev["evidence_ids"],
        )
    )
    return {
        "incident_id": incident["incident_id"],
        "pack": pack["pack"],
        "site_id": incident["site_id"],
        "valid": card is not None,
        "error": err or "",
        "latency_ms": latency_ms,
        "prompt_chars": len(system_prompt(pack)) + len(user_prompt(ev)),
        "evidence_json": json.dumps(ev, default=str),
        "raw": raw,
        **(card or {}),
    }


__all__ = [
    "SMALL_MODEL_PREFERENCE",
    "card_json_schema",
    "draft_card",
    "evidence_pack",
    "pick_model",
    "stub_model",
    "system_prompt",
    "user_prompt",
]
