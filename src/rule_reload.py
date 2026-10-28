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
