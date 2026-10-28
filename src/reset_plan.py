"""
Safety logic for the "[RTM] Reset Demo" job. Pure (no Spark, no network) so every guard is unit-tested.

The job is destructive by nature, so it is a dry run unless the job parameter is exactly RESET, it
refuses to run against a non-dev catalog, it refuses while any other demo job is running, and it only
ever touches the schema and checkpoint paths configured for the demo (nothing comes from free text).
"""

from __future__ import annotations

CONFIRM_WORD = "RESET"
OWN_JOB_MARKER = "Reset Demo"          # this job's own (active) run must not block itself


class ResetRefused(Exception):
    """A safety rail stopped the reset; the message says why."""


def is_confirmed(value: str | None) -> bool:
    """Only the exact word RESET confirms (no case-insensitivity, no surrounding spaces)."""
    return value == CONFIRM_WORD


def assert_dev_catalog(catalog: str) -> None:
    if not catalog.endswith("_dev"):
        raise ResetRefused(
            f"Refusing to reset catalog '{catalog}': the reset only runs against a catalog whose name ends in '_dev'."
        )


def active_demo_runs(run_names: list[str]) -> list[str]:
    """Names of other demo ([RTM]) runs that are still active, excluding this reset job itself."""
    return [n for n in run_names if "[RTM]" in n and OWN_JOB_MARKER not in n]


def assert_nothing_running(run_names: list[str]) -> None:
    busy = active_demo_runs(run_names)
    if busy:
        raise ResetRefused(
            "Refusing to reset while demo jobs are running (dropping tables under a live stream breaks it): "
            + "; ".join(busy) + ". Cancel those runs first."
        )


def build_plan(schema: str, tables: dict[str, int | None], checkpoints: list[str], confirm: str | None) -> dict:
    """
    schema: fully qualified schema name; tables: table name -> row count (None if unknown);
    checkpoints: checkpoint folders that exist. Returns what will (or would) be removed.
    """
    return {
        "mode": "execute" if is_confirmed(confirm) else "dry-run",
        "schema": schema,
        "tables": dict(tables),
        "checkpoints": list(checkpoints),
    }


def describe_plan(plan: dict) -> str:
    lines = []
    if plan["mode"] == "dry-run":
        lines.append("DRY RUN: nothing has been changed. To really reset, run this job again with confirm = RESET")
    else:
        lines.append("RESET CONFIRMED: removing the following")
    lines.append("")
    if plan["tables"]:
        lines.append(f"Schema {plan['schema']} and its {len(plan['tables'])} table(s):")
        for name, rows in sorted(plan["tables"].items()):
            lines.append(f"  - {name}" + (f"  ({rows:,} rows)" if rows is not None else ""))
    else:
        lines.append(f"Schema {plan['schema']}: not present (nothing to drop)")
    lines.append("")
    if plan["checkpoints"]:
        lines.append("Streaming checkpoint folder(s):")
        lines += [f"  - {p}" for p in plan["checkpoints"]]
    else:
        lines.append("Streaming checkpoint: not present (nothing to remove)")
    lines += ["", "Afterwards the next pipeline run seeds the rules registry and starts from empty."]
    return "\n".join(lines)
