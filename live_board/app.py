"""
Live Data Quality Loop — self-refreshing demo board.

Run on the presenter's laptop (signed in with the Databricks CLI profile):

    pip install -r live_board/requirements.txt
    streamlit run live_board/app.py

It reads the pipeline's Delta tables through the Databricks SQL connector and
updates itself every few seconds — no browser refresh needed. By default it
queries the all-purpose cluster the pipeline already uses (no extra compute).
To stop a forgotten tab from keeping that cluster awake, it auto-pauses after
a configurable number of minutes (Resume restarts the timer).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # repo root, for src/ and live_board/

import altair as alt
import pandas as pd
import streamlit as st

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
from src.status_view import collect_fast, collect_slow, fmt_int, pct, short_time

st.set_page_config(page_title="Live Data Quality Loop", layout="wide")

# ---------------------------------------------------------------------------
# Sidebar controls and session state
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


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render(snap: dict, prev: dict | None, seconds_left: int | None) -> None:
    total = snap["clean"] + snap["quarantine"]
    new_events = total - ((prev["clean"] + prev["quarantine"]) if prev else total)

    st.title("Live Data Quality Loop")
    state = f"auto-pauses in {seconds_left // 60} min" if (ss.live and seconds_left is not None) else "paused"
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

    st.subheader("AI suggestions")
    sc = snap["sugg_counts"]
    s1, s2, s3 = st.columns(3)
    s1.metric("Waiting for review", sc.get("pending", 0))
    s2.metric("Approved", sc.get("approved", 0))
    s3.metric("Rejected", sc.get("rejected", 0))
    if snap["pending"]:
        st.dataframe(
            pd.DataFrame(snap["pending"]).rename(columns={"name": "suggested rule", "criticality": "severity"}),
            hide_index=True, width="stretch",
        )
    note = snap["last_notification"]
    if note:
        st.caption(f"Last review alert at {short_time(note['created_at'])} UTC (+{note['new_count']} new)")

    st.info("**Your turn:** Jobs ▸ [RTM] Review Rule ▸ Run now with different parameters  →  "
            "`action = approve | reject`, `rule = <suggested rule name>`")


# ---------------------------------------------------------------------------
# Self-refreshing section
# ---------------------------------------------------------------------------

@st.fragment(run_every=f"{interval}s" if ss.live else None)
def board() -> None:
    stop, seconds_left = pause_status(ss.started, now(), max_minutes)
    if ss.live and stop:
        ss.live = False
        st.rerun()                      # full rerun drops the timer

    if not ss.live:
        st.warning("Updates are paused (to avoid keeping the cluster awake). Use **Resume updates** in the sidebar.")
        if ss.prev is not None:
            render(ss.prev, None, None)
        return

    try:
        runner = get_runner(target)
        fast = collect_fast(runner)
        if should_refresh_slow(ss.slow_at, now()):
            ss.slow, ss.slow_at = collect_slow(runner), now()
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
    ss.prev = snap
    render(snap, prev, seconds_left)


board()
