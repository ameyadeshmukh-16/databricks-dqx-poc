"""
Read-only, one-screen status view of the streaming loop, shared by the one-shot
"[RTM] Live Status" job and the live_monitor task inside the streaming pipeline.

Layout (<= ~62 columns so nothing wraps in the run's log pane):
  1 EVENTS IN -> 2 RULES ENFORCED -> 3 AI SUGGESTIONS -> 4 YOUR TURN
The screen is built from a plain snapshot dict by pure functions (unit-tested);
only collect_snapshot touches Spark.
"""

from __future__ import annotations

from datetime import datetime, timezone

from config.constants import (
    TABLE_BATCH_METRICS,
    TABLE_REVIEW_NOTIFICATIONS,
    TABLE_RULE_METRICS,
    TABLE_RULES,
    TABLE_RULES_STAGING,
    TABLE_STREAM_CLEAN,
    TABLE_STREAM_QUARANTINE,
)

INNER_WIDTH = 56          # characters between the banner's side borders
BAR_WIDTH = 10
TOP_RULES = 5
TOP_PENDING = 3
BATCHES_SHOWN = 6


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def fmt_int(n: int) -> str:
    return f"{int(n):,}"


def pct(part: float, total: float) -> float:
    """Percentage of total (0 when total is 0)."""
    return 100.0 * part / total if total else 0.0


def bar(percent: float, width: int = BAR_WIDTH) -> str:
    """Filled/empty block bar for a 0-100 percentage."""
    percent = max(0.0, min(100.0, percent))
    filled = round(width * percent / 100.0)
    if percent > 0 and filled == 0:
        filled = 1          # a non-zero value should always be visible
    return "█" * filled + "░" * (width - filled)


def recent_pcts(values: list[float], n: int = BATCHES_SHOWN) -> str:
    """The last n percentages as plain numbers, oldest to newest ('56% 55% 57%')."""
    return " ".join(f"{v:.0f}%" for v in values[-n:])


def short_time(ts) -> str:
    """HH:MM for a datetime (or '-' when missing)."""
    return ts.strftime("%H:%M") if ts is not None else "-"


def trunc(text: str, width: int) -> str:
    text = str(text)
    return text if len(text) <= width else text[: width - 1] + "…"


def user_part(email: str | None) -> str:
    """'jane.doe@corp.com' -> 'jane.doe' (compact, keeps the audit story visible)."""
    return (email or "unknown").split("@")[0]


def rules_applied(prev: dict | None, cur: dict) -> list[str]:
    """Names of rules that became active since the previous snapshot (sorted)."""
    if prev is None:
        return []
    return sorted(set(cur["rules"]) - set(prev["rules"]))


def since_last(prev: dict | None, cur: dict) -> list[str]:
    """Human-readable changes between two snapshots (empty list = no change)."""
    if prev is None:
        return []
    parts = []
    new_events = (cur["clean"] + cur["quarantine"]) - (prev["clean"] + prev["quarantine"])
    if new_events > 0:
        parts.append(f"+{fmt_int(new_events)} events")
    applied = rules_applied(prev, cur)
    if applied:
        shown = ", ".join(trunc(n, 22) for n in applied[:2])
        extra = f" +{len(applied) - 2} more" if len(applied) > 2 else ""
        parts.append(f"rule applied: {shown}{extra}")
    new_sugg = cur["sugg_counts"].get("pending", 0) - prev["sugg_counts"].get("pending", 0)
    if new_sugg > 0:
        parts.append(f"+{new_sugg} suggestions")
    return parts


# ---------------------------------------------------------------------------
# Screen layout (pure)
# ---------------------------------------------------------------------------

def _banner(ts: datetime) -> list[str]:
    title = "  LIVE DATA QUALITY LOOP"
    right = f"{ts:%H:%M:%S} UTC  "
    middle = " " * (INNER_WIDTH - len(title) - len(right))
    return [
        "╔" + "═" * INNER_WIDTH + "╗",
        "║" + title + middle + right + "║",
        "╚" + "═" * INNER_WIDTH + "╝",
    ]


def build_screen(snap: dict, prev: dict | None = None) -> str:
    """Render one snapshot as a compact screen; prev adds a 'since last' line."""
    lines = _banner(snap["ts"])

    # 1 — events in
    total = snap["clean"] + snap["quarantine"]
    if total == 0:
        lines.append(" 1  EVENTS IN       waiting for the first events…")
    else:
        new_events = total - ((prev["clean"] + prev["quarantine"]) if prev else total)
        suffix = f"   (+{fmt_int(new_events)} since last)" if prev and new_events > 0 else ""
        lines.append(f" 1  EVENTS IN       {fmt_int(total)} total{suffix}")
        c_pct, q_pct = pct(snap["clean"], total), pct(snap["quarantine"], total)
        lines.append(f"    ✔ clean       {fmt_int(snap['clean']):>7} {c_pct:4.0f}%  {bar(c_pct)}")
        lines.append(f"    ✖ quarantined {fmt_int(snap['quarantine']):>7} {q_pct:4.0f}%  {bar(q_pct)}")
        if snap["batch_pcts"]:
            lines.append(f"    quarantined per batch: {recent_pcts(snap['batch_pcts'])}")
    lines.append("")

    # 2 — rules enforced
    active = set(snap["rules"])
    reviewed = {r["name"]: r for r in snap["reviewed"] if r["name"] in active}
    lines.append(f" 2  RULES ENFORCED ({len(snap['rules'])} active) · caught, last 5 batches")
    seeded_firing = [(n, p) for n, p in snap["firing"].items() if n not in reviewed][:TOP_RULES]
    for name, p in seeded_firing:
        lines.append(f"    {trunc(name, 24):<24} {bar(p)} {p:5.1f}%")
    if not seeded_firing:
        lines.append("    (no rule has caught an event yet)")
    quiet = len([n for n in snap["rules"] if n not in reviewed]) - len(seeded_firing)
    if quiet > 0:
        lines.append(f"    +{quiet} more active rule{'s' if quiet != 1 else ''}, quiet")
    if reviewed:
        lines.append("    ★ ADDED BY HUMAN REVIEW")
        for r in list(reviewed.values())[:3]:
            caught = snap["firing"].get(r["name"])
            effect = f"catching {caught:.1f}%" if caught else "no violations yet"
            lines.append(f"      {trunc(r['name'], 26)}  {effect}")
            lines.append(f"        approved {short_time(r['reviewed_at'])} by {trunc(user_part(r['reviewed_by']), 26)}")
    lines.append("")

    # 3 — AI suggestions
    sc = snap["sugg_counts"]
    lines.append(f" 3  AI SUGGESTIONS  {sc.get('pending', 0)} waiting · ✔ {sc.get('approved', 0)} approved"
                 f" · ✖ {sc.get('rejected', 0)} rejected")
    for p in snap["pending"][:TOP_PENDING]:
        lines.append(f"    • {trunc(p['name'], 26):<26} [{p['criticality']}] {p['source']}")
    more = sc.get("pending", 0) - min(TOP_PENDING, len(snap["pending"]))
    note = snap["last_notification"]
    tail = []
    if more > 0:
        tail.append(f"+{more} more")
    if note:
        tail.append(f"last alert {short_time(note['created_at'])} (+{note['new_count']} new)")
    if tail:
        lines.append("    " + " · ".join(tail))
    lines.append("")

    # 4 — human decision
    lines.append(" 4  YOUR TURN: Jobs ▸ [RTM] Review Rule ▸ Run now")
    lines.append("    parameters: action=approve|reject  rule=<name>")

    changes = since_last(prev, snap)
    if prev is not None:
        lines.append("")
        lines.append(trunc("▲ since last snapshot: " + (" · ".join(changes) if changes else "no change"), 64))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------
# Reads go through a tiny "runner" so the same queries serve the Spark-based
# monitor/job and the laptop live board (SQL connector):
#   runner.query(sql) -> list[dict]      runner.table_exists(name) -> bool

HISTORY_BATCHES = 40


class SparkRunner:
    """Runner backed by a Spark session (used inside Databricks jobs)."""

    def __init__(self, spark):
        self._spark = spark

    def query(self, sql: str) -> list[dict]:
        return [r.asDict() for r in self._spark.sql(sql).collect()]

    def table_exists(self, name: str) -> bool:
        return self._spark.catalog.tableExists(name)


def _event_counts(runner) -> tuple[int, int]:
    """(clean, quarantine) row counts; one round trip when both tables exist."""
    has_clean = runner.table_exists(TABLE_STREAM_CLEAN)
    has_quarantine = runner.table_exists(TABLE_STREAM_QUARANTINE)
    if has_clean and has_quarantine:
        row = runner.query(
            f"SELECT (SELECT COUNT(*) FROM {TABLE_STREAM_CLEAN}) AS clean, "
            f"(SELECT COUNT(*) FROM {TABLE_STREAM_QUARANTINE}) AS quarantine"
        )[0]
        return int(row["clean"]), int(row["quarantine"])
    clean = int(runner.query(f"SELECT COUNT(*) AS n FROM {TABLE_STREAM_CLEAN}")[0]["n"]) if has_clean else 0
    quarantine = (int(runner.query(f"SELECT COUNT(*) AS n FROM {TABLE_STREAM_QUARANTINE}")[0]["n"])
                  if has_quarantine else 0)
    return clean, quarantine


def collect_fast(runner, last_batches: int = 8, history: int = HISTORY_BATCHES) -> dict:
    """The cheap, fast-changing numbers: event counts and the recent batch history."""
    clean, quarantine = _event_counts(runner)
    out = {
        "ts": datetime.now(tz=timezone.utc),
        "clean": clean,
        "quarantine": quarantine,
        "batch_history": [],
        "batch_pcts": [],
    }
    if runner.table_exists(TABLE_BATCH_METRICS):
        rows = runner.query(
            f"SELECT batch_id, quarantine_count, total_count, processed_at FROM {TABLE_BATCH_METRICS} "
            f"ORDER BY batch_id DESC LIMIT {history}"
        )
        out["batch_history"] = [
            {
                "batch_id": int(r["batch_id"]),
                "quarantine_pct": pct(r["quarantine_count"], r["total_count"]),
                "total": int(r["total_count"]),
                "processed_at": r["processed_at"],
            }
            for r in reversed(rows)
        ]
        out["batch_pcts"] = [b["quarantine_pct"] for b in out["batch_history"][-last_batches:]]
    return out


def collect_slow(runner, run_config_name: str = "streaming_event_rules") -> dict:
    """Rules, what is firing, reviewed rules and the suggestion queue (change less often)."""
    out = {
        "rules": [],
        "firing": {},
        "reviewed": [],
        "sugg_counts": {},
        "pending": [],
        "last_notification": None,
    }

    if runner.table_exists(TABLE_RULES):
        out["rules"] = [
            r["name"] for r in runner.query(
                f"SELECT name FROM {TABLE_RULES} WHERE run_config_name = '{run_config_name}' ORDER BY created_at, name"
            )
        ]

    if runner.table_exists(TABLE_RULE_METRICS):
        rows = runner.query(f"""
            WITH last_runs AS (
                SELECT run_id, MAX(recorded_at) AS t, MAX(total_records) AS total
                FROM {TABLE_RULE_METRICS}
                WHERE run_config_name = '{run_config_name}' AND run_type = 'streaming'
                GROUP BY run_id ORDER BY t DESC LIMIT 5
            ),
            v AS (
                SELECT m.rule_name, SUM(m.violations) AS viol
                FROM {TABLE_RULE_METRICS} m JOIN last_runs r ON m.run_id = r.run_id
                GROUP BY m.rule_name
            )
            SELECT v.rule_name, 100.0 * v.viol / (SELECT SUM(total) FROM last_runs) AS pct
            FROM v ORDER BY pct DESC
        """)
        out["firing"] = {r["rule_name"]: float(r["pct"]) for r in rows if r["pct"] is not None}

    if runner.table_exists(TABLE_RULES_STAGING):
        base = f"FROM {TABLE_RULES_STAGING} WHERE run_config_name = '{run_config_name}'"
        out["reviewed"] = runner.query(
            f"SELECT name, reviewed_by, reviewed_at {base} AND status = 'approved' "
            f"ORDER BY reviewed_at DESC LIMIT 5"
        )
        out["sugg_counts"] = {
            r["status"]: int(r["n"]) for r in runner.query(f"SELECT status, COUNT(*) AS n {base} GROUP BY status")
        }
        out["pending"] = runner.query(
            f"SELECT rule_id, name, function, column_name, constraint, criticality, message, source, "
            f"llm_backend, confidence, reasoning {base} AND status = 'pending' ORDER BY suggested_at DESC LIMIT 10"
        )

    if runner.table_exists(TABLE_REVIEW_NOTIFICATIONS):
        rows = runner.query(
            f"SELECT created_at, new_count FROM {TABLE_REVIEW_NOTIFICATIONS} "
            f"WHERE run_config_name = '{run_config_name}' ORDER BY created_at DESC LIMIT 1"
        )
        if rows:
            out["last_notification"] = rows[0]

    return out


def collect_snapshot(runner, run_config_name: str = "streaming_event_rules", last_batches: int = 8) -> dict:
    """Full snapshot = fast + slow parts. Missing tables give empty sections, never errors."""
    return {**collect_fast(runner, last_batches), **collect_slow(runner, run_config_name)}


def render_snapshot(spark, run_config_name: str = "streaming_event_rules",
                    last_batches: int = 8, prev: dict | None = None) -> dict:
    """Print the one-screen view and return the snapshot (pass it back as prev for the delta line)."""
    snap = collect_snapshot(SparkRunner(spark), run_config_name, last_batches)
    print(build_screen(snap, prev), flush=True)
    return snap
