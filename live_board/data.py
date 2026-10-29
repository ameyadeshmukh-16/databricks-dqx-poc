"""
Data layer for the live demo board (runs on the presenter's laptop).

- SqlRunner: reads the pipeline's Delta tables through the Databricks SQL
  connector, signed in with the existing Databricks CLI profile (OAuth), against
  either the already-running all-purpose cluster (default, no extra compute) or
  the SQL warehouse.
- Pure helpers that shape a status snapshot (src/status_view.py) into chart-ready
  rows, and the auto-pause arithmetic. These need neither Spark nor the network.
"""

from __future__ import annotations

import csv
import io
import os
import re
import time
from datetime import timezone

from src.status_view import rules_applied, short_time, user_part

# ---------------------------------------------------------------------------
# Settings (this POC's workspace; override with environment variables)
# ---------------------------------------------------------------------------
PROFILE = os.environ.get("BOARD_PROFILE", "dqx-poc-dev")
CLUSTER_ID = os.environ.get("BOARD_CLUSTER_ID", "1026-013008-6rqlr2d7")
WAREHOUSE_ID = os.environ.get("BOARD_WAREHOUSE_ID", "2562a89119bf9f67")

SLOW_TTL_SECONDS = 30          # rules / suggestions change rarely: re-read at most this often
BANNER_SECONDS = 120           # how long a "rule applied" banner stays up


def http_path(target: str, host: str) -> str:
    """HTTP path for the chosen compute ('cluster' or 'warehouse')."""
    if target == "warehouse":
        return f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    match = re.search(r"adb-(\d+)", host)
    if not match:
        raise ValueError(f"Cannot derive the workspace id from host '{host}'; set BOARD_HTTP_PATH")
    return f"sql/protocolv1/o/{match.group(1)}/{CLUSTER_ID}"


class SqlRunner:
    """Runner (query / table_exists) over the Databricks SQL connector."""

    def __init__(self, target: str = "cluster", profile: str = PROFILE):
        from databricks import sql                      # imported here so tests do not need the package
        from databricks.sdk.core import Config

        cfg = Config(profile=profile or None)          # empty profile = the environment's own login (cluster run)
        host = cfg.host.replace("https://", "").rstrip("/")
        self.target = target
        self.http_path = os.environ.get("BOARD_HTTP_PATH") or http_path(target, host)
        self._conn = sql.connect(
            server_hostname=host,
            http_path=self.http_path,
            credentials_provider=lambda: cfg.authenticate,
        )
        self._known: set[str] = set()

    def query(self, statement: str) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute(statement)
            return [row.asDict() for row in cur.fetchall()]

    def table_exists(self, name: str) -> bool:
        if name in self._known:
            return True
        try:
            self.query(f"SELECT 1 FROM {name} LIMIT 1")
        except Exception:                                # noqa: BLE001 - missing table (yet)
            return False
        self._known.add(name)
        return True

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Pure shaping helpers
# ---------------------------------------------------------------------------

def batch_chart_rows(snapshot: dict) -> list[dict]:
    """Quarantine % per micro-batch, oldest to newest."""
    return [{"batch": b["batch_id"], "quarantine_pct": round(b["quarantine_pct"], 1)}
            for b in snapshot["batch_history"]]


def rules_chart_rows(snapshot: dict, top: int = 8) -> list[dict]:
    """
    Share of events each rule caught (descending). Rules added through human review are
    labelled so the chart can colour them; rules that are not active are left out.
    """
    active = set(snapshot["rules"])
    human = {r["name"] for r in snapshot["reviewed"]} & active
    rows = [{"rule": name, "caught_pct": round(p, 1), "added_by": "human review" if name in human else "seeded"}
            for name, p in snapshot["firing"].items() if name in active]
    # a human-added rule that has not fired yet still deserves a (zero) bar
    rows += [{"rule": name, "caught_pct": 0.0, "added_by": "human review"}
             for name in sorted(human) if name not in snapshot["firing"]]
    rows.sort(key=lambda r: (-r["caught_pct"], r["rule"]))
    return rows[:top]


def reviewed_cards(snapshot: dict, limit: int = 3) -> list[dict]:
    """Human-approved rules that are live, with who/when and what they are catching."""
    active = set(snapshot["rules"])
    cards = []
    for r in snapshot["reviewed"]:
        if r["name"] in active:
            cards.append({
                "name": r["name"],
                "approved_at": short_time(r["reviewed_at"]),
                "by": user_part(r["reviewed_by"]),
                "caught_pct": snapshot["firing"].get(r["name"]),
            })
    return cards[:limit]


def banner_for(prev: dict | None, cur: dict) -> str | None:
    """Text for the 'rule applied' banner when a rule just became active."""
    applied = rules_applied(prev, cur)
    if not applied:
        return None
    return "Rule applied to the live stream: " + ", ".join(applied)


def pause_status(started_at: float, now: float, max_minutes: float) -> tuple[bool, int]:
    """(should_pause, seconds_left) for the auto-pause cost guard."""
    left = int(max_minutes * 60 - (now - started_at))
    return left <= 0, max(left, 0)


def should_refresh_slow(last_read_at: float | None, now: float, ttl: float = SLOW_TTL_SECONDS) -> bool:
    return last_read_at is None or now - last_read_at >= ttl


def now() -> float:
    return time.time()


# ---------------------------------------------------------------------------
# Latest quarantined events and the decision log
# ---------------------------------------------------------------------------

DASH = "—"


def _naive_utc(ts):
    """A datetime without tzinfo, in UTC (rows from different readers may or may not carry a timezone)."""
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def _clock(ts) -> str:
    ts = _naive_utc(ts)
    return ts.strftime("%H:%M:%S") if ts else DASH


def _or_dash(value):
    return DASH if value is None or value == "" else value


def quarantine_table_rows(snapshot: dict) -> list[dict]:
    """The latest quarantined events with the rule(s) that caught each and the reason, ready for a table."""
    rows = []
    for r in snapshot.get("recent_quarantine", []):
        price = r.get("unit_price")
        rows.append({
            "time (UTC)": _clock(r.get("ingested_at")),
            "event": _or_dash(r.get("event_id")),
            "store": _or_dash(r.get("store_id")),
            "division": _or_dash(r.get("division")),
            "qty": _or_dash(r.get("quantity")),
            "price": f"{price:.2f}" if isinstance(price, (int, float)) else DASH,
            "caught by": _or_dash(r.get("rules")),
            "why": _or_dash(r.get("reason")),
        })
    return rows


def seconds_to_first_catch(decision: dict) -> int | None:
    """Seconds from the decision until the rule first caught an event (None if it has not, or never will)."""
    decided, caught = _naive_utc(decision.get("reviewed_at")), _naive_utc(decision.get("first_caught_at"))
    if decided is None or caught is None:
        return None
    return max(0, int((caught - decided).total_seconds()))


def _decision_label(status: str) -> str:
    return {"approved": "Approved", "rejected": "Rejected"}.get(status, str(status).title())


def decision_rows(snapshot: dict) -> list[dict]:
    """Decisions for the screen: reviewer shortened to the user name, times as a clock."""
    rows = []
    for d in snapshot.get("decisions", []):
        seconds = seconds_to_first_catch(d)
        rows.append({
            "decided (UTC)": _clock(d.get("reviewed_at")),
            "decision": _decision_label(d.get("status")),
            "rule": d.get("name"),
            "severity": _or_dash(d.get("criticality")),
            "suggested by": _or_dash(d.get("llm_backend") or ("statistical analysis" if d.get("source") == "statistical" else d.get("source"))),
            "reviewer": user_part(d.get("reviewed_by")),
            "first caught after": f"{seconds} s" if seconds is not None else DASH,
        })
    return rows


CSV_COLUMNS = ("decided_at_utc", "decision", "rule", "severity", "source", "suggested_by",
               "reviewer", "first_caught_at_utc", "seconds_to_first_catch")


def decision_log_csv(snapshot: dict) -> str:
    """The audit evidence: full reviewer identity and exact timestamps, standard CSV quoting."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for d in snapshot.get("decisions", []):
        decided, caught = _naive_utc(d.get("reviewed_at")), _naive_utc(d.get("first_caught_at"))
        seconds = seconds_to_first_catch(d)
        writer.writerow([
            decided.isoformat(sep=" ", timespec="seconds") if decided else "",
            d.get("status", ""), d.get("name", ""), d.get("criticality", ""), d.get("source", ""),
            d.get("llm_backend") or "", d.get("reviewed_by") or "",
            caught.isoformat(sep=" ", timespec="seconds") if caught else "",
            "" if seconds is None else seconds,
        ])
    return out.getvalue()
