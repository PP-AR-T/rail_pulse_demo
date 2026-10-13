"""Load, validate and expand domain packs. Pure Python: no Spark here.

A pack is configuration, not code. validate_pack() is strict on purpose so a mistake in a YAML
file fails at load time with a readable message, not as a wrong answer in the control room.
"""

import random
import re
from pathlib import Path

from .contracts import AGGREGATE_COLUMNS, SEVERITY_RANK

EFFECT_KINDS = {
    "drift": {"per_min"},  # value += per_min * minutes since fault start
    "step": {"delta"},  # value += delta
    "spike": {"probability", "delta"},  # with this probability per reading, value += delta
    "flatline": set(),  # value frozen at nominal (stuck sensor)
    "dropout": set(),  # no readings at all (telemetry loss)
}

_RULE_WORDS = {"and", "or", "not", "abs", *AGGREGATE_COLUMNS}
_RULE_TOKEN = re.compile(r"\s+|[A-Za-z_]\w*|\d+(?:\.\d+)?(?:[eE]-?\d+)?|<=|>=|!=|=|<|>|[()+\-*/]")


class PackError(ValueError):
    pass


def load_yaml(path: str | Path) -> dict:
    try:
        import yaml
    except ImportError as e:  # pragma: no cover - only on a runtime without PyYAML
        raise PackError("PyYAML is not installed; run %pip install pyyaml") from e
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def check_rule(expr: str) -> str:
    """Allow only aggregate columns, numbers, comparisons and AND/OR/NOT in a detector rule.

    Rules are compiled into Spark SQL, so this is also an injection guard.
    """
    pos = 0
    while pos < len(expr):
        m = _RULE_TOKEN.match(expr, pos)
        if not m:
            raise PackError(f"rule {expr!r}: unexpected text at {expr[pos : pos + 12]!r}")
        token = m.group(0)
        if token[0].isalpha() or token[0] == "_":
            if token.lower() not in _RULE_WORDS:
                raise PackError(
                    f"rule {expr!r}: {token!r} is not allowed; use {sorted(AGGREGATE_COLUMNS)}"
                )
        pos = m.end()
    return expr


def validate_pack(pack: dict) -> dict:
    def need(cond, msg):
        if not cond:
            raise PackError(f"pack {pack.get('pack', '?')}: {msg}")

    for key in ("pack", "sites", "asset_types", "causes", "actions", "detectors", "agent"):
        need(key in pack, f"missing section {key!r}")
    need(re.fullmatch(r"[a-z][a-z0-9_]*", str(pack["pack"])), "pack id must be lower_snake_case")

    site_ids = [s["id"] for s in pack["sites"]]
    need(len(site_ids) == len(set(site_ids)), "duplicate site ids")

    measures = {}
    for at, spec in pack["asset_types"].items():
        need(spec.get("per_site", 0) >= 1, f"asset type {at}: per_site must be >= 1")
        need(spec.get("measures"), f"asset type {at}: no measures")
        for site in spec.get("sites", []):
            need(site in site_ids, f"asset type {at}: unknown site {site}")
        for m, ms in spec["measures"].items():
            need(m not in measures, f"measure {m} is defined twice; measure names must be unique")
            for k in ("unit", "period_s", "nominal", "noise"):
                need(k in ms, f"measure {m}: missing {k}")
            need(int(ms["period_s"]) >= 1, f"measure {m}: period_s must be >= 1")
            measures[m] = at

    cause_ids = [c["id"] for c in pack["causes"]]
    action_ids = [a["id"] for a in pack["actions"]]
    need(len(set(cause_ids)) == len(cause_ids), "duplicate cause ids")
    need(len(set(action_ids)) == len(action_ids), "duplicate action ids")

    det_ids = set()
    for d in pack["detectors"]:
        need(d["id"] not in det_ids, f"duplicate detector {d['id']}")
        det_ids.add(d["id"])
        need(
            d["measure"] == "*" or d["measure"] in measures,
            f"detector {d['id']}: unknown measure {d['measure']}",
        )
        need(
            d["severity"] in SEVERITY_RANK,
            f"detector {d['id']}: severity must be {list(SEVERITY_RANK)}",
        )
        need(d.get("meaning"), f"detector {d['id']}: add a 'meaning' so the agent can explain it")
        check_rule(d["when"])

    for s in pack.get("scenarios", []):
        need(s["cause"] in cause_ids, f"scenario {s['id']}: cause {s['cause']} is not in causes")
        t = s["target"]
        for site in t["sites"]:
            need(site in site_ids, f"scenario {s['id']}: unknown site {site}")
        for at in t["asset_types"]:
            need(at in pack["asset_types"], f"scenario {s['id']}: unknown asset type {at}")
        apt = t.get("assets_per_type", 1)
        need(
            apt == "all" or (isinstance(apt, int) and apt >= 1),
            f"scenario {s['id']}: assets_per_type must be 'all' or a positive integer",
        )
        for m, eff in s["effects"].items():
            need(
                measures.get(m) in t["asset_types"],
                f"scenario {s['id']}: measure {m} does not belong to a targeted asset type",
            )
            need(
                eff.get("kind") in EFFECT_KINDS, f"scenario {s['id']}: unknown effect kind for {m}"
            )
            missing = EFFECT_KINDS[eff["kind"]] - set(eff)
            need(not missing, f"scenario {s['id']}: effect on {m} needs {sorted(missing)}")

    for k in pack.get("knowledge", []):
        for d in k["applies_to"]:
            need(d in det_ids, f"knowledge note refers to unknown detector {d}")
    return pack


def load_pack(path: str | Path) -> dict:
    return validate_pack(load_yaml(path))


def load_packs(packs_dir: str | Path, names: list[str] | None = None) -> dict[str, dict]:
    """Load every *.yml in a folder (or only the named ones), keyed by pack id."""
    packs = {}
    for p in sorted(Path(packs_dir).glob("*.yml")):
        pack = load_pack(p)
        if names is None or pack["pack"] in names:
            packs[pack["pack"]] = pack
    if names:
        missing = set(names) - set(packs)
        if missing:
            raise PackError(f"no pack file found for {sorted(missing)} in {packs_dir}")
    return packs


# ---- expansion: pack -> concrete sensors and planted faults -------------------------------------


def sensor_catalogue(pack: dict, scale: int = 1) -> list[dict]:
    """Every sensor the simulator will produce, one dict per sensor. Size grows with scale."""
    site_name = {s["id"]: s["name"] for s in pack["sites"]}
    rows = []
    for site in pack["sites"]:
        for at, spec in pack["asset_types"].items():
            if "sites" in spec and site["id"] not in spec["sites"]:
                continue  # this asset type only exists at the listed sites
            for i in range(1, spec["per_site"] * scale + 1):
                asset_id = f"{spec['id_prefix']}-{site['id']}-{i:04d}"
                for m, ms in spec["measures"].items():
                    rows.append(
                        {
                            "pack": pack["pack"],
                            "site_id": site["id"],
                            "site_name": site_name[site["id"]],
                            "asset_id": asset_id,
                            "asset_type": at,
                            "sensor_id": f"{asset_id}.{m}",
                            "measure": m,
                            "unit": ms["unit"],
                            "period_s": int(ms["period_s"]),
                            "nominal": float(ms["nominal"]),
                            "noise": float(ms["noise"]),
                        }
                    )
    return rows


def fault_plan(pack: dict, catalogue: list[dict], seed: int = 7) -> list[dict]:
    """Expand scenarios into one row per affected sensor (the ground truth for scoring)."""
    rng = random.Random(seed)
    plan = []
    for s in pack.get("scenarios", []):
        t = s["target"]
        for site in t["sites"]:
            for at in t["asset_types"]:
                assets = sorted(
                    {
                        c["asset_id"]
                        for c in catalogue
                        if c["site_id"] == site and c["asset_type"] == at
                    }
                )
                if not assets:
                    raise PackError(
                        f"pack {pack['pack']}: scenario {s['id']} targets {at} "
                        f"at {site}, but that site has none"
                    )
                n = t.get("assets_per_type", 1)
                chosen = assets if n == "all" else rng.sample(assets, min(n, len(assets)))
                for c in catalogue:
                    if c["asset_id"] in chosen and c["measure"] in s["effects"]:
                        eff = s["effects"][c["measure"]]
                        plan.append(
                            {
                                "pack": pack["pack"],
                                "scenario_id": s["id"],
                                "cause": s["cause"],
                                "site_id": site,
                                "asset_id": c["asset_id"],
                                "sensor_id": c["sensor_id"],
                                "measure": c["measure"],
                                "kind": eff["kind"],
                                "per_min": float(eff.get("per_min", 0.0)),
                                "delta": float(eff.get("delta", 0.0)),
                                "probability": float(eff.get("probability", 0.0)),
                                "start_min": int(s["start_min"]),
                                "end_min": int(s["start_min"]) + int(s["duration_min"]),
                            }
                        )
    seen = set()
    for row in plan:
        if row["sensor_id"] in seen:
            raise PackError(
                f"pack {pack['pack']}: sensor {row['sensor_id']} is targeted by more than one "
                "scenario; give each scenario its own assets"
            )
        seen.add(row["sensor_id"])
    return plan


def detector_index(pack: dict) -> dict[str, dict]:
    return {d["id"]: d for d in pack["detectors"]}


def knowledge_for(pack: dict, detector_ids: list[str]) -> list[str]:
    wanted = set(detector_ids)
    return [k["text"] for k in pack.get("knowledge", []) if wanted & set(k["applies_to"])]


def readings_per_hour(catalogue: list[dict]) -> int:
    return sum(3600 // c["period_s"] for c in catalogue)
