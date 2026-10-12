# Rail Pulse on Databricks — today's run sheet (Mon 12 Oct, 13:00–16:30)

Goal by 16:00: one decision end to end on Databricks (signal → agent proposal → human approval → score), three screenshots or a 60-second recording, and a LinkedIn post published. Every step also touches something on the Generative AI Engineer Associate syllabus; those are marked **[cert]**.

Hard rules for today: synthetic data, labelled; no Kafka or Rust today (Free Edition is serverless and can't reach a local broker — this is the file-fed Databricks variant); if a step runs over, cut it and move on. The post goes out at 15:30 whatever state the stretch goal is in.

## 13:00–13:15 — Set up (15 min)

1. Open the Databricks Free Edition workspace. Workspace → Import → upload `rail_pulse_day1.py` (it imports as a notebook). Attach to serverless.
2. Run cells 0 and 1. Expect: catalog `rail_pulse`, schema `demo`, tables `movements` (≈100 rows) and `timetable`.
   - If `CREATE CATALOG` is refused, set `CATALOG = "workspace"` in cell 0 and re-run.
3. Catalog Explorer → confirm the two tables exist. **[cert: Unity Catalog as the governance layer for AI data]**

## 13:15–13:35 — Signals and the heatmap (20 min)

1. Run cell 2. Expect "18 cascade signals" (or similar) — the first at Southampton Central around 09:45.
2. Run the heatmap cell. On the `display()` output click the visualisation icon → Heatmap: X = window, Y = station, value = mean_lateness. Save the visualisation.
3. **Screenshot 1**: the heatmap. The diagonal streak from Southampton to Waterloo is the cascade.

## 13:35–14:15 — The agent step (40 min) **[cert: Foundation Model APIs, prompt design, structured output, guardrails, MLflow tracing]**

1. Run the endpoint-listing cell. Pick a model from the list and set `DATABRICKS_MODEL` in cell 0 (default `databricks-meta-llama-3-3-70b-instruct`; use `databricks-claude-sonnet-4` if listed). Re-run cell 0.
2. Run the agent cell. Expect two proposals, each either `valid=True` with the JSON printed, or `valid=False` with the reason. An invalid one is not a failure — it is the guardrail working, and it's worth a screenshot too.
3. Open the MLflow experiment linked at the top right of the notebook (or Experiments → the notebook's experiment) → Traces tab. Each model call is traced with the prompt, the evidence and the response.
4. **Screenshot 2**: the proposal JSON next to its trace.
5. If the model returns prose instead of JSON twice in a row: lower `temperature` to 0, or add "Begin your answer with {" to the system prompt. Don't spend more than 10 minutes here.

## 14:15–14:35 — Human gate and scoring (20 min) **[cert: human-in-the-loop, evaluation metrics]**

1. Run cell 4 once to create the widgets (top of the notebook). Set `proposal_id` = first proposal, `decision` = approve, `reason` = "evidence consistent with the windows". Run cell 4 again.
2. Set `proposal_id` = second proposal, `decision` = edit, `edited_delay_band` = a different band, `reason` = "band looks optimistic given the trend". Run cell 4 again. (Or reject it with a reason — either way the log shows a human overriding the model.)
3. Run cells 5 and 6. Expect precision, recall and band_hit per decision. At least one imperfect score is good: that's the honesty of the lane.
4. **Screenshot 3**: the decision log (cell 6).

## 14:35–15:05 — Certification stretch, pick ONE (30 min, hard cap)

- **A. Evaluation run [cert: MLflow evaluation, custom scorers].** New cell:
  ```python
  import mlflow
  eval_df = proposals[["proposal_id", "raw", "valid"]].rename(columns={"raw": "outputs"})
  with mlflow.start_run(run_name="day1-proposals"):
      mlflow.log_metric("valid_json_rate", float(proposals.valid.mean()))
      mlflow.log_metric("mean_precision", float(scores.precision.mean()))
      mlflow.log_metric("mean_recall", float(scores.recall.mean()))
      mlflow.log_metric("band_hit_rate", float(scores.band_hit.mean()))
      mlflow.log_table(eval_df, "proposals.json")
  ```
  Then open the run in the MLflow UI. If `mlflow.genai.evaluate` is available in the workspace, wrap `valid_json` as a custom scorer instead — that is the exam's shape.
- **B. Tool as a Unity Catalog function [cert: UC functions as agent tools].** New SQL cell:
  ```sql
  CREATE OR REPLACE FUNCTION rail_pulse.demo.upcoming_services(station STRING, after TIMESTAMP)
  RETURNS TABLE (service_id STRING, planned_ts TIMESTAMP)
  RETURN SELECT service_id, planned_ts FROM rail_pulse.demo.timetable
         WHERE timetable.station = upcoming_services.station AND planned_ts >= after AND planned_ts < after + INTERVAL 60 MINUTES;
  ```
  Call it from SQL, then note in the README that the Agent Framework version binds this function as a tool. Don't build the Agent Framework version today.

## 15:05–15:30 — Save the work (25 min)

1. Notebook → File → Export → Source file. Save to the rail repo as `databricks/rail_pulse_day1.py`. Commit: "Databricks variant, day 1: signals, agent proposal, human gate, scorer on synthetic replay".
2. Add to the rail README a section "Databricks variant (day 1)" with the "What's real and what isn't" table from the last cell, and a line: "Synthetic data. The real replay file arrives with M0."
3. Put the three screenshots in `docs/day1/`.

## 15:30–16:00 — Post (30 min)

1. Open `POST.md`. Adjust the numbers to what you actually got (precision/recall, which proposal was wrong). Do not round up.
2. Attach the three screenshots as a carousel (or the 60-second recording: Win+Alt+R starts Game Bar recording; walk through heatmap → proposal → widgets → decision log).
3. Publish. Reply to every comment today.

## 16:00–16:30 — Buffer

Anything that slipped, or one course module. Then stop.

## If something blocks

- No foundation model endpoints listed → set `MODEL_BACKEND = "anthropic"` and paste a Claude API key into the `anthropic_api_key` widget (created by cell 4 — run cell 4 once first, or create it with `dbutils.widgets.text("anthropic_api_key","")`). Never commit the key.
- Serverless unavailable → the notebook runs on any small cluster; nothing here needs Spark beyond saving tables.
- Running out of time → the post still goes out with whatever cells ran; the text in `POST.md` has a "partial" variant.
