"""
Pure logic for the pre-flight check (scripts/preflight.py): result model, evaluation of what
was observed, and the printed report. Nothing here talks to Databricks, so it is unit-tested.
"""

from __future__ import annotations

from dataclasses import dataclass

OK, INFO, WARN, FAIL = "ok", "info", "warn", "fail"
_MARKERS = {OK: "[ OK ]", INFO: "[INFO]", WARN: "[WARN]", FAIL: "[FAIL]"}

REQUIRED_SECRETS = ("azure-storage-key", "eventhub-conn-str", "openrouter-api-key")
OPTIONAL_SECRETS = ("slack-webhook-url",)

# job display-name suffixes the demo relies on (name = "[dev <user>] [RTM] <suffix>")
REQUIRED_JOBS = ("Kafka Streaming Pipeline", "Review Rule", "Live Status")
OPTIONAL_JOBS = ("Seed Rules Registry", "Batch DQX Pipeline", "DQ Alert Check (Streaming)", "Reset Demo")


@dataclass
class Check:
    name: str
    status: str
    detail: str = ""


# ---------------------------------------------------------------------------
# evaluation helpers
# ---------------------------------------------------------------------------

def normalize_text(content: bytes | str) -> str:
    """Line-ending- and trailing-whitespace-insensitive form used to compare local and deployed files."""
    text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    return text.replace("\r\n", "\n").replace("\r", "\n").rstrip()


def compare_files(local: dict[str, str], remote: dict[str, str | None]) -> dict[str, list[str]]:
    """
    local / remote map relative path -> normalised content (remote None = not deployed).
    Returns {"changed": [...], "missing": [...]} (sorted).
    """
    changed, missing = [], []
    for path, text in local.items():
        deployed = remote.get(path)
        if deployed is None:
            missing.append(path)
        elif deployed != text:
            changed.append(path)
    return {"changed": sorted(changed), "missing": sorted(missing)}


def evaluate_deployed_code(diff: dict[str, list[str]], total: int) -> Check:
    problems = diff["changed"] + diff["missing"]
    if not problems:
        return Check("Deployed code matches the repo", OK, f"{total} files identical")
    shown = ", ".join(problems[:6]) + (f" … (+{len(problems) - 6} more)" if len(problems) > 6 else "")
    return Check("Deployed code matches the repo", FAIL,
                 f"{len(diff['changed'])} differ, {len(diff['missing'])} not deployed: {shown}. "
                 f"Run: databricks bundle deploy -t dev -p <profile>")


def evaluate_secrets(present: set[str], scope: str = "dqx-poc") -> list[Check]:
    checks = []
    missing = [s for s in REQUIRED_SECRETS if s not in present]
    checks.append(
        Check(f"Secrets in scope '{scope}'", FAIL, "missing: " + ", ".join(missing)) if missing
        else Check(f"Secrets in scope '{scope}'", OK, ", ".join(REQUIRED_SECRETS)))
    for name in OPTIONAL_SECRETS:
        checks.append(
            Check(f"Optional secret '{name}'", OK, "present") if name in present
            else Check(f"Optional secret '{name}'", WARN, "not set: Slack alerts will not be delivered (logged only)"))
    return checks


def evaluate_jobs(job_names: list[str]) -> list[Check]:
    def has(suffix: str) -> bool:
        return any(n.endswith(f"[RTM] {suffix}") for n in job_names)
    missing_required = [s for s in REQUIRED_JOBS if not has(s)]
    missing_optional = [s for s in OPTIONAL_JOBS if not has(s)]
    checks = [Check("Demo jobs deployed", FAIL, "missing: " + ", ".join(missing_required) + ". Run bundle deploy.")
              if missing_required else Check("Demo jobs deployed", OK, ", ".join(REQUIRED_JOBS))]
    if missing_optional:
        checks.append(Check("Other jobs", WARN, "not deployed: " + ", ".join(missing_optional)))
    return checks


def evaluate_cluster(state: str | None, started_by_us: bool = False) -> Check:
    if state == "RUNNING":
        return Check("Cluster", OK, "running" + (" (started by this check)" if started_by_us else ""))
    if state in ("PENDING", "RESTARTING", "RESIZING"):
        return Check("Cluster", WARN, f"{state.lower()}: wait for it to be RUNNING")
    return Check("Cluster", FAIL, f"{(state or 'unknown').lower()}: re-run with --start-cluster (takes a few minutes)")


def evaluate_warehouse(state: str | None) -> Check:
    if state in (None, "STOPPED", "DELETED"):
        return Check("SQL warehouse", OK, "stopped (no cost)")
    return Check("SQL warehouse", WARN, f"{state.lower()}: it bills while on; stop it unless you need it")


def parse_git_status(porcelain: str) -> dict[str, list[str]]:
    """`git status --porcelain` output -> tracked changes (these can differ from what is deployed) and untracked files."""
    tracked, untracked = [], []
    for line in porcelain.splitlines():
        if not line.strip():
            continue
        (untracked if line.startswith("??") else tracked).append(line[3:].strip())
    return {"tracked": tracked, "untracked": untracked}


def evaluate_git(status: dict[str, list[str]], ahead: int = 0) -> Check:
    problems = []
    if status["tracked"]:
        problems.append(f"{len(status['tracked'])} uncommitted change(s): " + ", ".join(status["tracked"][:4]))
    if ahead:
        problems.append(f"{ahead} commit(s) not pushed")
    if problems:
        return Check("Git working tree", WARN, "; ".join(problems))
    note = f" ({len(status['untracked'])} untracked file(s) ignored)" if status["untracked"] else ""
    return Check("Git working tree", OK, "clean and pushed" + note)


def evaluate_environment(state: dict) -> Check:
    """state: counts read from the tables (None = table missing)."""
    rules = state.get("registry_rules")
    pending = state.get("pending")
    clean, quarantine = state.get("stream_clean"), state.get("stream_quarantine")
    if rules is None and clean is None:
        return Check("Environment", INFO, "empty catalog: the pipeline's first task will create and seed it")
    parts = [f"{rules or 0} streaming rule(s) in the registry",
             f"{pending or 0} suggestion(s) pending",
             f"{(clean or 0) + (quarantine or 0):,} stream event(s) stored"]
    dirty = bool(pending) or (clean or 0) + (quarantine or 0) > 0
    suffix = " — leftovers from earlier runs; run [RTM] Reset Demo for a from-scratch demo" if dirty else ""
    return Check("Environment", INFO, "; ".join(parts) + suffix)


def pinned_models(bundle: dict) -> str:
    """The comma-separated llm_model default from a parsed databricks.yml ('' if none)."""
    try:
        return str(bundle["variables"]["llm_model"]["default"] or "")
    except (KeyError, TypeError):
        return ""


def deep_check_code(models: str) -> str:
    """Python run on the cluster by --deep: can it reach Event Hubs, and does the pinned AI model answer?"""
    lines = [
        "import json, os, socket, time",
        "out = {}",
        "host, port = os.environ['KAFKA_BOOTSTRAP_SERVERS'].rsplit(':', 1)",
        "t = time.time()",
        "try:",
        "    socket.create_connection((host, int(port)), timeout=8).close()",
        "    out['eventhub'] = 'reachable from the cluster (%.1fs)' % (time.time() - t)",
        "except Exception as e:",
        "    out['eventhub_error'] = str(e)[:120]",
        f"os.environ['OPENROUTER_MODEL'] = {models!r}",
        "from ai_suggester.llm_backend import describe_backend, get_backend",
        "t = time.time()",
        "try:",
        "    llm = get_backend('openrouter')",
        "    llm.generate('Reply with the single word: ok')",
        "    out['llm'] = '%s answered in %.0fs' % (describe_backend(llm), time.time() - t)",
        "except Exception as e:",
        "    out['llm_error'] = str(e).splitlines()[0][:140]",
        "print('@@RESULT@@' + json.dumps(out))",
    ]
    return chr(10).join(lines) + chr(10)


def evaluate_deep(result: dict) -> list[Check]:
    checks = []
    checks.append(Check("Event Hubs reachable", OK, result["eventhub"]) if "eventhub" in result
                  else Check("Event Hubs reachable", FAIL, result.get("eventhub_error", "no answer")))
    checks.append(Check("AI model answers", OK, result["llm"]) if "llm" in result
                  else Check("AI model answers", FAIL, result.get("llm_error", "no answer")))
    return checks


# ---------------------------------------------------------------------------
# result handling
# ---------------------------------------------------------------------------

def exit_code(checks: list[Check]) -> int:
    return 1 if any(c.status == FAIL for c in checks) else 0


def format_report(checks: list[Check]) -> str:
    width = max((len(c.name) for c in checks), default=0)
    lines = [f"{_MARKERS[c.status]} {c.name:<{width}}  {c.detail}".rstrip() for c in checks]
    fails = sum(c.status == FAIL for c in checks)
    warns = sum(c.status == WARN for c in checks)
    lines.append("")
    if fails:
        lines.append(f"NOT READY: {fails} problem(s), {warns} warning(s). Fix the [FAIL] lines first.")
    elif warns:
        lines.append(f"READY with {warns} warning(s): read them before you present.")
    else:
        lines.append("READY: all checks passed.")
    return "\n".join(lines)
