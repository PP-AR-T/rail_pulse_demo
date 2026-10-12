# LinkedIn post — Mon 12 Oct (post 0: building in the open, day 1)

Replace the bracketed numbers with what you actually got. Don't round up. Attach the three screenshots (heatmap, proposal + trace, decision log) as a carousel, or the 60-second recording.

---

Building in the open, day 1.

For years I've built metadata-driven data platforms for regulators and utilities. The rule that kept them maintainable was simple: a new source is configuration, not code, and lineage is built in rather than assumed.

I'm now applying the same rules to decisions on live data: an agent proposes, a human signs, every decision is logged and scored.

Today, three hours on Databricks, alongside the Generative AI Engineer course:

→ synthetic train movements on the Bournemouth–Waterloo corridor, with a planted points failure at Southampton
→ 15-minute lateness windows; a cascade signal when late trains start accumulating
→ an agent drafts a passenger advisory from the evidence — strict JSON, or it's rejected
→ I approve one and override the other
→ an hour later each decision is scored against what "actually" happened: precision [0.6], recall [0.75], delay band [hit/missed]

One of the proposals was wrong. That stays on the board. The score is the product.

Synthetic data, one notebook, nothing production. The real feed, the Rust edge and the streaming job come next; this is the file-fed Databricks variant.

What decision would you let an agent propose if a human had to sign it?

#DataEngineering #Databricks #AgenticAI #HumanInTheLoop

---

## Partial variant (if only the signal and proposal cells ran)

Building in the open, day 1.

For years I've built metadata-driven data platforms for regulators and utilities: a new source is configuration, not code, and lineage is built in. I'm now applying the same rules to decisions on live data — an agent proposes, a human signs, every decision is logged and scored.

Today, three hours on Databricks alongside the Generative AI Engineer course: synthetic train movements on the Bournemouth–Waterloo corridor, a planted points failure, 15-minute lateness windows, a cascade signal, and an agent drafting a passenger advisory from the evidence in strict JSON. Invalid output gets rejected, not retried quietly.

The human gate and the scoring are tomorrow's cells. Synthetic data, one notebook, nothing production.

What decision would you let an agent propose if a human had to sign it?

#DataEngineering #Databricks #AgenticAI #HumanInTheLoop

---

## Rules this post follows

- Stage-1 framing: foundations first, Live lane as "where this goes", built in the open.
- No client or employer names. No production claims. Synthetic labelled as synthetic.
- Real numbers only, including the bad one.
- Ends with one question.
- Reply to every comment the same day.
