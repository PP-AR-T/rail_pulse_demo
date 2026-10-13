# Rail Pulse — streaming decision demo (the accelerator, first cut)

**What it shows:** thousands of sensors streaming millions of readings, deterministic detection,
alarm storms collapsed into incidents, a small model drafting one card per incident, and a human
approving every card. The same code runs any domain: rail and water ship today.

```
 domain pack (YAML)  ──────────────┬──────────────────────┬──────────────────────┐
                                   ▼                      ▼                      ▼
 sensor_simulator ──▶ landing Volume ──▶ bronze ──▶ 1-min aggregates ──▶ detectors ──▶ incidents
   (or a real feed)    (Parquet)     Auto Loader   (per-pack watermark)   (pack rules)  (alarm storm
                                                                                         → 1 row)
                                                                                           │
                         operator console ◀── incident_cards ◀── small model + guardrails ◀┘
                         approve / dismiss / escalate → operator_decisions (audit trail)
```

**All data is synthetic** and every reading says so (`source = 'synthetic'`). Thresholds are
illustrative, not Network Rail or water company standards.

## What's where

| Piece | File | Domain-specific? |
|---|---|---|
| Contracts: reading schema, incident card + guardrails | `databricks/accelerator/contracts.py` | No |
| Pack loader, validator, rule injection guard | `databricks/accelerator/packs.py` | No |
| Simulator, aggregates, detectors, incidents (Spark) | `databricks/accelerator/spark_ops.py` | No |
| Evidence pack, prompt, small-model picker | `databricks/accelerator/agent.py` | No |
| Streaming pipeline | `databricks/pipelines/sensor_streaming.py` | No |
| Simulator / agent notebooks | `databricks/notebooks/sensor_simulator.py`, `incident_cards.py` | No |
| Operator console | `databricks/app/pages/operator_console.py` | No |
| **Rail pack** | `databricks/domain_packs/rail.yml` | **Yes** |
| **Water pack** | `databricks/domain_packs/water.yml` | **Yes** |

New domain = new YAML file. No code change.

## Measured locally (4-core laptop Spark, scale 1, 60 simulated minutes)

| Pack | Sensors | Readings | Anomalies | Incidents | Planted faults found |
|---|---|---|---|---|---|
| rail | 864 | 2,385,600 | 511 | 5 | 5 of 5, no false alarms |
| water | 24 | 14,760 | 95 | 3 | 3 of 3, no false alarms |

The Woking signalling power dip is the headline: **320 track-circuit alarms on 40 circuits
became one incident**, and the model is shown one summary line instead of 320 rows (about
3,500 prompt characters per card). Re-measure on Databricks and quote those numbers, not these.

Volume knob: `scale` multiplies assets per site. Rail ≈ 2.4 M readings per simulated hour at
scale 1, ≈ 24 M at scale 10. Start at 1 on Free Edition, then try 5.

## Run it (about 30 minutes the first time)

1. **Deploy the bundle.** Push to `main` (the workflow now also deploys on changes to
   `accelerator/`, `domain_packs/` and the two notebooks), or from `databricks/`:
   `databricks bundle deploy -t dev`.
2. **Run the job** `rail_pulse_sensor_demo` (Jobs → Run now). Three tasks:
   simulate (≈2.4 M rail readings) → `rail_pulse_sensors` pipeline → incident cards.
   - Want water instead? Run now with different parameters → `pack = water`.
   - No model endpoints yet? `backend = stub` runs everything; stub cards say `[STUB]`.
3. **Look at the pipeline graph** (Pipelines → rail_pulse_sensors): bronze → silver (one flow per
   pack) → anomalies → incidents. Expectations are on bronze. **Screenshot 1.**
4. **Open `incident_cards`** (the job run's last task output): cards, the alarm-compression table
   and the score against ground truth. Open the MLflow trace of one card. **Screenshot 2.**
5. **Operator console** (one-time setup): App → Edit → add resource *SQL warehouse*, key
   `sql-warehouse`, permission *Can use*. Grant the app's service principal access:
   ```sql
   GRANT USE CATALOG ON CATALOG rail_pulse TO `<app service principal>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA rail_pulse.ops TO `<app service principal>`;
   GRANT MODIFY ON TABLE rail_pulse.ops.operator_decisions TO `<app service principal>`;
   ```
   Redeploy the app. The sidebar now has **operator console**: approve one card with a reason,
   dismiss another. **Screenshot 3:** the decision log.
6. **Run the job again.** The simulator's clock continues where it stopped, Auto Loader picks up
   only the new files, and only new incidents get cards.

## Live-looking mode

Set `pause_s = 30` and `batch_minutes = 1` on the simulator, and while it drips files, run the
pipeline repeatedly (or set it to continuous if your workspace allows it). Each refresh shows new
readings, anomalies and incidents arriving.

## Talk track (60 seconds)

> 2.4 million readings an hour from 864 trackside sensors. Detection is plain rules a domain
> engineer can read, not AI. At Woking, 320 alarms from 40 track circuits arrive at once; the
> pipeline groups them into one incident, and a small model, given one line of evidence instead of
> 320 rows, names the likely cause: the shared signalling power supply. It can only pick causes and
> actions from a list the engineers wrote, and it must cite evidence it was shown, or the card is
> rejected. A controller approves or dismisses it with a reason. Swap the YAML file and the same
> pipeline runs a wastewater network.

## Honest limits (say them before someone else does)

- Synthetic data. The real lane needs a client's telemetry or the Network Rail open feeds.
- "Small model" today means the smallest pay-per-token model your workspace serves, grounded by
  the pack. Fine-tuning a domain model is a later step, once there are approved decisions to learn
  from: `operator_decisions` is that training set.
- Triggered pipeline, not continuous, because of Free Edition limits. The watermark holds back the
  last ~2 minutes of each pack until the next batch arrives.
- Sessions can merge: if two incidents at one site touch within 10 minutes they become one.
- Kafka/Redpanda and the Rust edge plug in at the landing step: anything that lands the reading
  contract works unchanged downstream.

## Tests

```
cd databricks
pip install pyspark pyyaml pytest   # Java 17+ for the Spark tests
pytest -q                           # 29 tests: packs, guardrails, and both packs end to end on local Spark
```
