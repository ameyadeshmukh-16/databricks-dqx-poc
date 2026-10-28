"""
Live Data Quality Loop — self-refreshing demo board with in-app review.

Run on the presenter's laptop (signed in with the Databricks CLI profile):

    pip install -r live_board/requirements.txt
    python -m streamlit run live_board/app.py

It reads the pipeline's Delta tables through the Databricks SQL connector and
updates itself every few seconds — no browser refresh needed. New AI rule
suggestions appear as cards that can be approved or rejected right here; the
decision runs on the cluster (same guards as the Review Rule job) and the stream
applies an approved rule on its next micro-batch.

By default it queries the all-purpose cluster the pipeline already uses (no extra
compute). To stop a forgotten tab from keeping that cluster awake, it auto-pauses
after a configurable number of minutes (Resume restarts the timer).
"""

from __future__ import annotations

import atexit
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # repo root, for src/ and live_board/

import altair as alt
import pandas as pd
import streamlit as st

from live_board.cluster_session import ReviewSession, ReviewSessionError
from live_board.data import (
    BANNER_SECONDS,
    SqlRunner,
    banner_for,
    batch_chart_rows,
    now,
    pause_status,
    reviewed_cards,
    rules_chart_rows,
    should_refresh_slow,
)
from live_board.impact import describe_impact, preview_rule
from src.rule_review import describe_rule, describe_severity
from src.status_view import collect_fast, collect_slow, fmt_int, pct, short_time

st.set_page_config(page_title="Live Data Quality Loop", layout="wide")

CONFIRM_TIMEOUT_SECONDS = 300      # an open confirmation holds the refresh timer at most this long
RESULT_SECONDS = 90                # how long the outcome of a decision stays on screen
MAX_CARDS = 8
PREVIEWS_PER_TICK = 2              # impact previews computed per refresh (keeps ticks light)
RUN_CONFIG = "streaming_event_rules"

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
ss = st.session_state
ss.setdefault("live", True)
ss.setdefault("started", now())
ss.setdefault("prev", None)
ss.setdefault("slow", None)
ss.setdefault("slow_at", None)
ss.setdefault("banner", None)
ss.setdefault("banner_at", 0.0)
ss.setdefault("last_error", None)
ss.setdefault("confirm", None)          # {"rule_id", "name"} while an approval awaits confirmation
ss.setdefault("confirm_at", 0.0)
ss.setdefault("timer_held", False)
ss.setdefault("last_review", None)      # {"ok", "message", "at"}
ss.setdefault("impact", {})             # rule_id -> preview result

with st.sidebar:
    st.header("Board settings")
    target = st.selectbox("Query compute", ["cluster", "warehouse"], index=0,
                          help="'cluster' reuses the running pipeline cluster (no extra cost). "
                               "'warehouse' uses the SQL warehouse, which bills while on.")
    interval = st.slider("Refresh every (seconds)", 5, 60, 10)
    max_minutes = st.number_input("Auto-pause after (minutes)", min_value=1, max_value=120, value=20)
    if ss.live:
        if st.button("Pause updates"):
            ss.live = False
            st.rerun()
    else:
        if st.button("Resume updates", type="primary"):
            ss.live = True
            ss.started = now()
            st.rerun()
    st.caption("Polling keeps the compute awake, so the board pauses itself after the time above "
               "and should be closed after the demo.")


@st.cache_resource(show_spinner="Connecting to Databricks…")
def get_runner(compute: str) -> SqlRunner:
    return SqlRunner(compute)


@st.cache_resource
def get_review_session() -> ReviewSession:
    session = ReviewSession()
    atexit.register(session.close)          # free the cluster-side session when the board exits
    _warm_in_background(session)
    return session


def _warm_in_background(session: ReviewSession) -> None:
    """Start the cluster session now so the first Approve click is fast."""
    def _go():
        try:
            session.warm_up()
        except Exception:                                    # noqa: BLE001 - surfaced when a decision is made
            pass
    threading.Thread(target=_go, daemon=True).start()


# ---------------------------------------------------------------------------
# Review actions
# ---------------------------------------------------------------------------

def _decide(action: str, rule_id: str) -> None:
    """Run one decision on the cluster and remember the outcome for display."""
    verb = ("Checking the rule on live events, then approving"
            if action == "approve" else "Rejecting")
    with st.spinner(f"{verb}… (about 15 s)"):
        try:
            result = get_review_session().review(action, rule_id, RUN_CONFIG)
            outcome = {"ok": result["ok"], "message": result["message"]}
        except ReviewSessionError as exc:
            outcome = {"ok": False, "message": f"Could not reach the cluster session ({exc}). "
                                               f"Use Jobs ▸ [RTM] Review Rule instead."}
    ss.last_review = {**outcome, "at": now()}
    ss.confirm = None
    ss.slow_at = None                  # re-read rules / suggestions on the next tick
    ss.impact.pop(rule_id, None)
    st.rerun()                         # full rerun restores the refresh timer


def render_queue(snap: dict) -> None:
    pending = snap["pending"][:MAX_CARDS]
    pending_ids = {p["rule_id"] for p in snap["pending"]}

    st.subheader("Review queue — your decision")
    if ss.last_review and now() - ss.last_review["at"] < RESULT_SECONDS:
        if ss.last_review["ok"]:
            st.success(f"✅ {ss.last_review['message']}")
        else:
            st.error(f"Not applied: {ss.last_review['message']}")

    if ss.confirm and (ss.confirm["rule_id"] not in pending_ids or now() - ss.confirm_at > CONFIRM_TIMEOUT_SECONDS):
        ss.confirm = None              # rule vanished or the confirmation went stale

    if ss.confirm:
        row = next((p for p in snap["pending"] if p["rule_id"] == ss.confirm["rule_id"]), None)
        if row:
            with st.container(border=True):
                st.warning("**Confirm approval** — this rule will be enforced on live events.")
                st.markdown(f"**{row['name']}** — {describe_rule(row)}")
                st.markdown(describe_severity(row["criticality"]))
                st.caption(describe_impact(ss.impact.get(row["rule_id"])))
                c1, c2, _ = st.columns([1, 1, 4])
                if c1.button("Confirm approval", type="primary", key="confirm_approve"):
                    _decide("approve", row["rule_id"])
                if c2.button("Cancel", key="cancel_approve"):
                    ss.confirm = None
                    st.rerun()

    if not pending:
        st.caption("No suggestions waiting. New ones appear here as the AI finds patterns in the quarantined events.")
        return

    for row in pending:
        rid = row["rule_id"]
        with st.container(border=True):
            st.markdown(f"**{row['name']}**  ·  severity: `{row['criticality']}`  ·  from `{row['source']}`")
            st.write(describe_rule(row))
            st.caption(describe_severity(row["criticality"]))
            made_by = row.get("llm_backend")
            st.caption(f"Suggested by: {made_by}" if made_by else "Suggested by: statistical analysis (no AI model)")
            why = row.get("reasoning") or row.get("message")
            if why:
                st.caption(f"Why: {why}")
            st.caption(describe_impact(ss.impact.get(rid)))
            b1, b2, _ = st.columns([1, 1, 4])
            if b1.button("Approve", key=f"approve_{rid}", type="primary", disabled=bool(ss.confirm)):
                ss.confirm = {"rule_id": rid, "name": row["name"]}
                ss.confirm_at = now()
                st.rerun()
            if b2.button("Reject", key=f"reject_{rid}", disabled=bool(ss.confirm)):
                _decide("reject", rid)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render(snap: dict, prev: dict | None, seconds_left: int | None) -> None:
    total = snap["clean"] + snap["quarantine"]
    new_events = total - ((prev["clean"] + prev["quarantine"]) if prev else total)

    st.title("Live Data Quality Loop")
    if ss.confirm:
        state = "refresh held while you confirm"
    elif ss.live and seconds_left is not None:
        state = f"auto-pauses in {seconds_left // 60} min"
    else:
        state = "paused"
    st.caption(f"Updated {snap['ts']:%H:%M:%S} UTC  ·  refreshes every {interval}s  ·  {state}")

    if ss.banner and now() - ss.banner_at < BANNER_SECONDS:
        st.success(f"★ {ss.banner}")

    if total == 0:
        st.info("Waiting for the first events… start the streaming pipeline to see data flow in.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events in", fmt_int(total), f"+{fmt_int(new_events)}" if prev and new_events > 0 else None)
    c2.metric("Clean", fmt_int(snap["clean"]), f"{pct(snap['clean'], total):.0f}% of events", delta_color="off")
    c3.metric("Quarantined", fmt_int(snap["quarantine"]),
              f"{pct(snap['quarantine'], total):.0f}% of events", delta_color="off")
    c4.metric("Rules enforced", len(snap["rules"]))

    left, right = st.columns(2)
    with left:
        st.subheader("Quarantine rate per micro-batch")
        rows = batch_chart_rows(snap)
        if rows:
            chart = (
                alt.Chart(pd.DataFrame(rows))
                .mark_line(point=True)
                .encode(
                    x=alt.X("batch:O", title="micro-batch"),
                    y=alt.Y("quarantine_pct:Q", title="% quarantined", scale=alt.Scale(domain=[0, 100])),
                    tooltip=["batch", "quarantine_pct"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, width="stretch")
        else:
            st.caption("No micro-batches yet.")
    with right:
        st.subheader("Rules enforced — share of events each rule caught")
        rows = rules_chart_rows(snap)
        if rows:
            chart = (
                alt.Chart(pd.DataFrame(rows))
                .mark_bar()
                .encode(
                    y=alt.Y("rule:N", sort="-x", title=None),
                    x=alt.X("caught_pct:Q", title="% of events (last 5 batches)"),
                    color=alt.Color("added_by:N", title="added by",
                                    scale=alt.Scale(domain=["seeded", "human review"],
                                                    range=["#4C78A8", "#F58518"])),
                    tooltip=["rule", "caught_pct", "added_by"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, width="stretch")
        else:
            st.caption("No rule has caught an event yet.")

    cards = reviewed_cards(snap)
    if cards:
        st.subheader("★ Added by human review")
        for c in cards:
            effect = f"catching {c['caught_pct']:.1f}% of events" if c["caught_pct"] else "no violations yet"
            st.success(f"**{c['name']}**  ·  approved {c['approved_at']} UTC by **{c['by']}**  ·  {effect}")

    sc = snap["sugg_counts"]
    s1, s2, s3 = st.columns(3)
    s1.metric("Suggestions waiting", sc.get("pending", 0))
    s2.metric("Approved", sc.get("approved", 0))
    s3.metric("Rejected", sc.get("rejected", 0))
    note = snap["last_notification"]
    if note:
        st.caption(f"Last review alert at {short_time(note['created_at'])} UTC (+{note['new_count']} new)")

    render_queue(snap)


# ---------------------------------------------------------------------------
# Self-refreshing section
# ---------------------------------------------------------------------------

def _update_previews(runner: SqlRunner, pending: list[dict]) -> None:
    """Compute a few missing impact previews per tick and drop ones for rules no longer pending."""
    ids = {p["rule_id"] for p in pending}
    for stale in [k for k in ss.impact if k not in ids]:
        del ss.impact[stale]
    todo = [p for p in pending[:MAX_CARDS] if p["rule_id"] not in ss.impact][:PREVIEWS_PER_TICK]
    for row in todo:
        ss.impact[row["rule_id"]] = preview_rule(runner, row)


@st.fragment(run_every=f"{interval}s" if (ss.live and not ss.confirm) else None)
def board() -> None:
    if bool(ss.confirm) != ss.timer_held:       # keep the timer in step with an open confirmation
        ss.timer_held = bool(ss.confirm)
        st.rerun()

    stop, seconds_left = pause_status(ss.started, now(), max_minutes)
    if ss.live and stop:
        ss.live = False
        get_review_session().close()
        st.rerun()                              # full rerun drops the timer

    if not ss.live:
        st.warning("Updates are paused (to avoid keeping the cluster awake). Use **Resume updates** in the sidebar.")
        if ss.prev is not None:
            render(ss.prev, None, None)
        return

    try:
        runner = get_runner(target)
        fast = collect_fast(runner)
        if should_refresh_slow(ss.slow_at, now()):
            ss.slow = collect_slow(runner)
            ss.slow_at = now()
            _update_previews(runner, ss.slow["pending"])
        snap = {**fast, **ss.slow}
        ss.last_error = None
    except Exception as exc:            # noqa: BLE001 - show it and retry on the next tick
        get_runner.clear()
        ss.last_error = str(exc).splitlines()[0][:300]
        st.error(f"Could not read from Databricks, will retry: {ss.last_error}")
        if ss.prev is not None:
            render(ss.prev, None, seconds_left)
        return

    prev = ss.prev
    text = banner_for(prev, snap)
    if text:
        ss.banner, ss.banner_at = text, now()
        st.toast(text, icon="✅")
    if prev is not None:
        new_waiting = snap["sugg_counts"].get("pending", 0) - prev["sugg_counts"].get("pending", 0)
        if new_waiting > 0:
            st.toast(f"{new_waiting} new rule suggestion{'s' if new_waiting != 1 else ''} waiting for review", icon="🔔")
    ss.prev = snap
    render(snap, prev, seconds_left)


board()
