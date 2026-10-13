"""Pure-Python tests for the accelerator core: packs, contracts, agent guardrails. No Spark."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from accelerator import agent as A  # noqa: E402
from accelerator import packs as P  # noqa: E402
from accelerator.contracts import validate_card  # noqa: E402

PACKS_DIR = ROOT / "domain_packs"


@pytest.fixture(scope="module")
def packs():
    return P.load_packs(PACKS_DIR)


def test_every_shipped_pack_is_valid(packs):
    assert {"rail", "water"} <= set(packs)


def test_rule_guard_rejects_anything_but_aggregates():
    P.check_rule("n >= 20 AND std_value < 0.000001")
    for bad in ["value > 1", "max_value > 1; DROP TABLE x", "max_value > '1'", "rand() > 0.5"]:
        with pytest.raises(P.PackError):
            P.check_rule(bad)


def test_pack_errors_are_readable(packs):
    broken = json.loads(json.dumps(packs["rail"]))
    broken["scenarios"][0]["cause"] = "gremlins"
    with pytest.raises(P.PackError, match="cause gremlins is not in causes"):
        P.validate_pack(broken)


def test_scale_multiplies_sensors(packs):
    one = P.sensor_catalogue(packs["rail"], 1)
    four = P.sensor_catalogue(packs["rail"], 4)
    assert len(four) == 4 * len(one)
    assert len({c["sensor_id"] for c in four}) == len(four)
    # 8 sites x (40 track circuits x 2 + 1 substation x 2) at 1 Hz dominates the volume
    assert P.readings_per_hour(one) > 2_000_000


def test_fault_plan_targets_what_the_pack_says(packs):
    cat = P.sensor_catalogue(packs["rail"], 1)
    plan = P.fault_plan(packs["rail"], cat, seed=7)
    by_scenario = {}
    for p in plan:
        by_scenario.setdefault(p["scenario_id"], set()).add(p["asset_id"])
    assert len(by_scenario["wok_signalling_power_dip"]) == 40  # assets_per_type: all
    assert len(by_scenario["sou_points_degradation"]) == 1
    assert all(
        p["site_id"] == "WOK" for p in plan if p["scenario_id"] == "wok_signalling_power_dip"
    )
    assert P.fault_plan(packs["rail"], cat, seed=7) == plan  # deterministic


ALLOWED = {
    "cause_ids": ["c1", "c2"],
    "action_ids": ["monitor", "dispatch"],
    "evidence_ids": ["d1", "A-1"],
}


def good_card(**over):
    card = {
        "headline": "Points slowing at SOU",
        "what_happened": "x",
        "likely_cause": "c1",
        "operational_impact": "y",
        "recommended_action": "dispatch",
        "urgency": "soon",
        "confidence": 0.7,
        "evidence": ["d1", "A-1"],
    }
    card.update(over)
    return card


def test_valid_card_passes():
    card, err = validate_card(good_card(), **ALLOWED)
    assert err is None and card["likely_cause"] == "c1"


@pytest.mark.parametrize(
    "over,reason",
    [
        ({"likely_cause": "aliens"}, "not an allowed cause"),
        ({"recommended_action": "open_the_points"}, "not an allowed action"),
        ({"evidence": ["d1", "made-up-asset"]}, "not in the evidence pack"),
        ({"confidence": 1.4}, "confidence out of range"),
        ({"urgency": "whenever"}, "urgency"),
        ({"headline": "x" * 200}, "headline"),
    ],
)
def test_guardrails_reject_bad_cards(over, reason):
    card, err = validate_card(good_card(**over), **ALLOWED)
    assert card is None and reason in err


def test_unknown_cause_is_allowed():
    assert validate_card(good_card(likely_cause="unknown"), **ALLOWED)[1] is None


def test_draft_card_with_stub_is_valid_and_compact(packs):
    pack = packs["rail"]
    incident = {
        "incident_id": "rail-WOK-x",
        "site_id": "WOK",
        "site_name": "Woking",
        "started_at": "2026-10-13 10:20:00",
        "last_seen_at": "2026-10-13 10:29:00",
        "anomaly_count": 400,
        "asset_count": 40,
        "asset_types": ["track_circuit"],
    }
    anomalies = [
        {
            "detector_id": "tc_low_voltage",
            "asset_id": f"TC-WOK-{i:04d}",
            "measure": "relay_voltage_v",
            "unit": "V",
            "min_value": 0.74,
            "max_value": 0.8,
            "window_start": f"2026-10-13 10:{20 + m}:00",
            "window_end": f"2026-10-13 10:{21 + m}:00",
        }
        for i in range(1, 41)
        for m in range(8)
    ]
    row = A.draft_card(incident, anomalies, pack, A.stub_model)
    assert row["valid"], row["error"]
    ev = json.loads(row["evidence_json"])
    assert len(ev["signals_by_detector"]) == 1  # 320 alarms summarised as one line
    assert ev["signals_by_detector"][0]["assets_affected"] == 40
    assert any("shared signalling power supply" in k for k in ev["knowledge"])
    assert row["prompt_chars"] < 6000  # small-model friendly


def test_failed_model_call_is_kept_as_a_rejected_row(packs):
    def down(system, user):
        raise TimeoutError("endpoint busy")

    incident = {
        "incident_id": "i",
        "site_id": "SOU",
        "site_name": "S",
        "started_at": "t",
        "last_seen_at": "t",
        "anomaly_count": 1,
        "asset_count": 1,
        "asset_types": [],
    }
    anomaly = {
        "detector_id": "points_slow_throw",
        "asset_id": "PM-SOU-0001",
        "measure": "throw_time_s",
        "unit": "s",
        "min_value": 4.3,
        "max_value": 4.4,
        "window_start": "a",
        "window_end": "b",
    }
    row = A.draft_card(incident, [anomaly], packs["rail"], down)
    assert not row["valid"] and "endpoint busy" in row["error"]


def test_pick_model_prefers_small_and_respects_override():
    served = ["databricks-meta-llama-3-3-70b-instruct", "databricks-meta-llama-3-1-8b-instruct"]
    assert A.pick_model(served) == "databricks-meta-llama-3-1-8b-instruct"
    assert A.pick_model(served, "databricks-meta-llama-3-3-70b-instruct").endswith("70b-instruct")
    with pytest.raises(ValueError):
        A.pick_model(["databricks-bge-large-en"])
