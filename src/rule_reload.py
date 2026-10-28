"""
Helpers for hot-reloading DQX rules inside a running stream.

The stream re-reads the rule registry each micro-batch; these helpers decide
what changed so it can be logged, and keep the previous rules when a reload fails.
"""

from __future__ import annotations

import json


def _signature(check: dict) -> str:
    """Canonical string of the parts of a check that define its behavior."""
    return json.dumps(
        {
            "criticality": check.get("criticality"),
            "check": check.get("check"),
            "filter": check.get("filter"),
        },
        sort_keys=True,
        default=str,
    )


def rule_set_changes(old: list[dict], new: list[dict]) -> tuple[list[str], list[str], list[str]]:
    """
    Compare two rule sets by name.

    Returns (added, removed, changed) rule-name lists, each sorted. `changed`
    means the name exists in both sets but its criticality / check / filter differs.
    """
    old_by_name = {c.get("name"): _signature(c) for c in old}
    new_by_name = {c.get("name"): _signature(c) for c in new}

    added = sorted(set(new_by_name) - set(old_by_name))
    removed = sorted(set(old_by_name) - set(new_by_name))
    changed = sorted(n for n in set(old_by_name) & set(new_by_name) if old_by_name[n] != new_by_name[n])
    return added, removed, changed


def fallback_culprits(last_good: list[dict], current: list[dict]) -> set[str]:
    """
    Names of the rules that differ between the last working rule set and the one that
    just failed to apply (added or changed). These are the suspects to block.
    """
    added, _removed, changed = rule_set_changes(last_good, current)
    return set(added) | set(changed)


def without_blocked(checks: list[dict], blocked: set[str]) -> list[dict]:
    """Drop rules the stream has already found it cannot apply."""
    return [c for c in checks if c.get("name") not in blocked]


# DQX does not raise on a check it cannot evaluate (unknown column, bad function...). It marks
# EVERY row as failing that check with a message like the ones below, which silently sends all
# events to quarantine. These helpers recognise that situation.
SKIPPED_MARKERS = ("evaluation skipped", "invalid sql expression")


def is_skipped_message(message: str | None) -> bool:
    """True when a DQX result message says the check could not be evaluated at all."""
    text = (message or "").lower()
    return any(marker in text for marker in SKIPPED_MARKERS)
