# DQX → DIEM: A Governed Data Quality Framework Opportunity

> **Purpose:** Present the case for extending our DQX proof-of-concept into the client's DIEM framework as a standardized, governed data quality capability.
> **Status:** Client-side technical validation received and positive. This brief consolidates that feedback into a proposed opportunity and phased plan.
> **Audience:** Internal leadership, for presentation to the client.

---

## 1. The Opportunity, in One Paragraph

The client's DIEM framework has a data quality gap it has already identified internally but not yet closed. Its legacy quality tooling (Blue Ribbon / Great Expectations) is frozen and no longer enhanced; the current recommended fallback — basic Databricks table constraints — is acknowledged by the client's own DIEM team as too limited for real use. Multiple client teams have independently flagged Databricks Labs DQX as the intended successor, and at least one team has already started adopting it ad hoc, without any shared governance. Our DQX proof-of-concept is not a speculative pitch into unclaimed territory — it is a working, validated answer to a gap the client has already named publicly in their own internal documentation. When we shared a technical summary of our POC with the client's own DIEM-side technical agent for an independent feasibility read, the response was unambiguous: **"yes, this is highly integrable — and the timing is actually excellent."**

---

## 2. Why the Timing Works in Our Favor

The client's own technical assessment surfaced four points that make this a live opportunity rather than a future one:

- **Blue Ribbon is officially in legacy status.** The client's Data Observability team has stopped enhancing it, and the quality package built around it is effectively frozen.
- **The client's own team acknowledges the current fallback is insufficient.** Databricks Constraints (simple NOT NULL / CHECK rules) is what's recommended today, and DIEM's own team has said as much internally.
- **Client teams are already asking for exactly this.** Internal client documentation explicitly states: *"It would be great if DIEM can have recommendations over Databricks constraints + tools like DQX by Databricks."*
- **A basic DQX example already exists inside the client's own quickstart repo** — a static YAML-file rule definition with a simple quarantine pattern. It has no rule lifecycle, no metrics, and no feedback loop. It is the seed of exactly what our POC has already built out fully.

In short: the client has already decided the direction (DQX), already started in a minimal way, and does not yet have a governed, production-grade version of it. We do.

---

## 3. What Our POC Adds That the Client's Framework Doesn't Have Today

This table reflects the client-side technical agent's own comparison of their current state against our working POC:

| Capability | Client's Current State | Our POC |
|---|---|---|
| Rule storage | YAML files checked into each pipeline's repo | Centralized Delta table (`dq_rules_registry`), shared across pipelines |
| Rule change deployment | Code change + PR + redeploy | Table update — live on the next run, no redeploy |
| Rule lifecycle governance | None — rules go live as soon as code merges | Staging → human review → promotion, with a full audit trail |
| AI-assisted rule authoring | Not present | Statistical profiler + pluggable LLM suggestions, with mandatory human approval before anything is enforced |
| Quarantine feedback loop | Not present | Mines quarantine failure patterns and suggests new rules to close the gaps |
| Per-rule effectiveness metrics | Not present | Dedicated metrics table + dashboard, including trend analysis and detection of rules that have stopped firing |
| Data-quality-specific alerting | Only generic infrastructure monitoring (DataDog / Dynatrace), not quality-aware | Spike detection against each rule's own rolling baseline, with deduplication so the same incident doesn't re-alert |
| Streaming data quality | Being designed, not yet implemented on the client side | Already working and validated in a live micro-batch streaming pattern |

This is the core of the pitch: every row in the right-hand column is either a capability gap the client has already named, or a maturity gap between "a YAML file exists" and "a governed system exists."

---

## 4. Proposed Phased Integration Model

The client's technical review proposed three phases, based on how directly each part of our POC maps onto an existing DIEM structure versus how much new ground it breaks. We're presenting that phasing as the basis for a staged engagement.

### Phase 1 — Core Rule Governance (Immediate Fit)

**What it replaces:** DIEM's current quality sub-package (`diem.quality`), which today wraps the legacy Blue Ribbon/Great Expectations tooling or static YAML rule files.

**What goes in:**
- The centralized rule registry (`dq_rules_registry`) as the single authoritative rule source
- The staging/review/promotion workflow (`dq_rules_staging`) as the governed path for any rule change
- The `DQEngine.load_checks(...)` integration point pipelines already use to read rules at runtime
- Structured quarantine output, with per-record error/warning detail attached automatically

**Why the client's own roadmap already calls for this:** their documented "Governance and operational quality" phase explicitly calls for defining "reusable data rule patterns" and deciding "quarantine behavior by severity." Our registry pattern answers both directly, out of the box.

**Adjustments needed to fit DIEM conventions** (per the client's own review):
- Catalog/database naming needs to conform to DIEM's namespace-driven convention rather than our POC's hardcoded values
- Table access should route through Unity Catalog external locations rather than direct storage-account keys, matching how DIEM already standardizes storage access
- Our rule-group naming (`run_config_name`) should align with DIEM's existing bundle-namespace convention

### Phase 2 — Observability Layer (Near-Term)

**What goes in:** the per-rule metrics table, the effectiveness dashboard, and the spike-detection alerting logic.

**Why it fits cleanly:** this layer slots in alongside the client's existing monitoring stack rather than competing with it. The metrics table is already designed to be queried by any BI tool, and the dashboard is built on native Databricks tooling — no new infrastructure required.

**Adjustments needed:**
- Alerting should route through the client's existing notification conventions instead of our POC's Slack webhook — the client's technical reviewer specifically pointed to their DataDog monitoring and Dynatrace instrumentation paths as the right target
- The alert-check job should be restructured to match the client's standard deployment-bundle layout

### Phase 3 — AI Rule Suggester & Quarantine Analyzer (Strategic Differentiator)

**What goes in:** the statistical profiler, the LLM-based rule suggester, and the quarantine-mining feedback loop.

**Why this is the standout:** the client's own reviewer was explicit that this is genuinely new ground — *"no existing framework capability covers it."* This is the part of our POC that isn't just filling a documented gap, it's introducing a capability the client hasn't built or scoped anywhere yet. Their assessment suggested this could become its own new addition to their framework specifically because of that.

**Adjustments / open questions the client will need to resolve:**
- LLM access needs to route through whatever governed-secrets or centralized LLM-access pattern the client's security/governance process requires — direct API keys in environment variables, as in our current POC, won't clear an enterprise governance review as-is
- The client's team will need to decide whether AI-suggested rules are opt-in or on-by-default per pipeline — our human-review gate makes either choice safe, but it's a policy decision for their side, not a technical blocker

---

## 5. Adapting Our POC to DIEM's Conventions

The client's technical reviewer mapped each of our POC's current environment-specific dependencies to a specific DIEM-side convention it should adopt instead. This is useful evidence that the gap between "our POC" and "a DIEM-native capability" is a set of known, addressable substitutions — not an open-ended unknown.

| Our POC's Current Dependency | What the Client Says to Adopt Instead |
|---|---|
| Hardcoded Unity Catalog names | DIEM's namespace variables in its deployment config |
| A fixed, pre-provisioned cluster ID | DIEM's approved cluster policies, via a standard job-cluster spec |
| Azure Event Hubs-specific streaming setup | DIEM's existing connector pattern for Kafka/Event Hubs, which the client says already exists |
| Direct storage-account keys | Unity Catalog external locations plus DIEM's standard secret-scope and session-auth handling |
| Slack webhook alerts | DIEM's existing DataDog/Dynatrace alerting conventions |
| Raw LLM API keys in environment variables | Databricks secret scopes DIEM already uses for service-principal credentials, with a future centralized LLM gateway as a likely next step on the client's side |
| Single-environment deployment bundle | DIEM's standard multi-environment (dev → stage → prod) deployment pipeline |

None of these are architectural blockers. They are the specific, itemizable set of substitutions needed to move our proof-of-concept from "runs for our team" to "runs as a DIEM-native capability" — and the client's own technical review already named the target for each one.

---

## 6. One Known Limitation, Already Flagged as Non-Blocking

Our streaming pipeline loads its rule set once when it starts, so a newly approved rule takes effect on the next restart rather than mid-stream. This is a real, documented tradeoff in our current design. The client's own reviewer flagged it explicitly — and concluded that it satisfies their stated requirement for non-blocking data quality checks in streaming pipelines, while noting the client may eventually want a hot-reload option in a future iteration. Their framing was to treat this as a "Phase 2 enhancement," not a blocker to moving forward.

---

## 7. Organizational Demand Signal

This isn't only a technical fit — there's already internal pull at the client. Per the client's own technical review, other teams within their organization have already published implementation guides and integration patterns for DQX independently, without a shared governance layer behind any of them. Our POC is positioned to be the piece that turns that scattered, ad hoc adoption into a single governed pattern the whole organization can standardize on.

---

## 8. Suggested Framing for the Client Conversation

The client's own technical reviewer suggested leading with this framing, and we agree it's the strongest version of the pitch:

> Blue Ribbon is legacy, Databricks Constraints are too simple, and multiple teams are already adopting DQX ad hoc with no governance. This proof-of-concept gives DIEM a governed, centralized DQX pattern — table-driven rules with a promotion lifecycle — that can become the standardized quality-package replacement. The AI and dashboard layers are additional value on top of that core.

---

## 9. Recommended Next Step

Before committing to a specific timeline or level of effort, we recommend a scoping pass to size Phase 1 concretely: what it takes to re-point our POC's naming, storage access, and deployment conventions at DIEM's actual standards (not just the categories named above, but the specific configuration values), and what a minimal Phase 1 pilot with one client pipeline would look like. We do not yet have estimation data to put a timeline against the phases above, and we'd rather scope that properly than present a guess as a plan.
