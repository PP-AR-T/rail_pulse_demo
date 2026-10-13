"""The two contracts everything else plugs into.

1. Reading: one sensor value at one time. Every source (simulator, a real feed, a Kafka topic)
   must land data in this shape. Detection never sees anything else.
2. Incident card: what the agent proposes to an operator. Validated strictly before anyone sees it.
"""

import json
import re

# ---- 1. Reading ---------------------------------------------------------------------------------

READING_FIELDS = [
    ("pack", "STRING"),  # which domain pack produced or describes it
    ("site_id", "STRING"),
    ("site_name", "STRING"),
    ("asset_id", "STRING"),
    ("asset_type", "STRING"),
    ("sensor_id", "STRING"),  # asset_id + measure
    ("measure", "STRING"),
    ("unit", "STRING"),
    ("event_ts", "TIMESTAMP"),
    ("value", "DOUBLE"),
    ("quality", "STRING"),  # good | suspect
    ("source", "STRING"),  # synthetic | <feed name>
    ("run_id", "STRING"),  # lineage back to the run or poll that produced it
]
READING_DDL = ", ".join(f"{name} {typ}" for name, typ in READING_FIELDS)

# Columns of the 1-minute aggregate that detector rules may reference.
AGGREGATE_COLUMNS = ("n", "avg_value", "min_value", "max_value", "std_value")

SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}

# ---- 2. Incident card ---------------------------------------------------------------------------

URGENCY = ("routine", "soon", "immediate")
UNKNOWN_CAUSE = "unknown"

CARD_FIELDS = {
    "headline": str,  # one line an operator reads first
    "what_happened": str,  # plain-language summary of the evidence
    "likely_cause": str,  # a cause id from the pack, or "unknown"
    "operational_impact": str,
    "recommended_action": str,  # an action id from the pack
    "urgency": str,  # routine | soon | immediate
    "confidence": float,  # 0..1
    "evidence": list,  # detector and asset ids taken from the evidence pack
}
HEADLINE_MAX = 120


def card_json_schema(cause_ids: list[str], action_ids: list[str]) -> dict:
    """JSON schema for the card, for models that support structured output."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(CARD_FIELDS),
        "properties": {
            "headline": {"type": "string", "maxLength": HEADLINE_MAX},
            "what_happened": {"type": "string"},
            "likely_cause": {"type": "string", "enum": [*cause_ids, UNKNOWN_CAUSE]},
            "operational_impact": {"type": "string"},
            "recommended_action": {"type": "string", "enum": list(action_ids)},
            "urgency": {"type": "string", "enum": list(URGENCY)},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 1},
        },
    }


def validate_card(
    obj: dict, *, cause_ids: list[str], action_ids: list[str], evidence_ids: list[str]
) -> tuple[dict | None, str | None]:
    """Check a parsed card against the contract and the evidence it was given.

    Returns (card, None) when valid, otherwise (None, reason). The reason is stored with the
    rejected card so a guardrail failure is visible, not silently dropped.
    """
    if not isinstance(obj, dict):
        return None, "card is not a JSON object"
    for key, typ in CARD_FIELDS.items():
        if key not in obj:
            return None, f"missing key {key}"
        if typ is float and isinstance(obj[key], int) and not isinstance(obj[key], bool):
            obj[key] = float(obj[key])
        if not isinstance(obj[key], typ):
            return None, f"key {key} should be {typ.__name__}"
    extra = set(obj) - set(CARD_FIELDS)
    if extra:
        return None, f"unexpected keys {sorted(extra)}"
    if not obj["headline"].strip() or len(obj["headline"]) > HEADLINE_MAX:
        return None, f"headline must be 1-{HEADLINE_MAX} characters"
    if obj["likely_cause"] not in [*cause_ids, UNKNOWN_CAUSE]:
        return None, f"likely_cause {obj['likely_cause']!r} is not an allowed cause"
    if obj["recommended_action"] not in action_ids:
        return None, f"recommended_action {obj['recommended_action']!r} is not an allowed action"
    if obj["urgency"] not in URGENCY:
        return None, f"urgency must be one of {URGENCY}"
    if not 0 <= obj["confidence"] <= 1:
        return None, "confidence out of range"
    if not obj["evidence"]:
        return None, "evidence is empty"
    unknown = [e for e in obj["evidence"] if e not in set(evidence_ids)]
    if unknown:
        # Grounding guardrail: the model may only cite ids it was actually shown.
        return None, f"evidence cites ids not in the evidence pack: {unknown[:5]}"
    return obj, None


def parse_card(raw: str, **allowed) -> tuple[dict | None, str | None]:
    """Extract the first JSON object from a model response and validate it."""
    match = re.search(r"\{.*\}", raw or "", re.S)
    if not match:
        return None, "no JSON object in response"
    try:
        obj = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        return None, f"invalid JSON: {e}"
    return validate_card(obj, **allowed)
