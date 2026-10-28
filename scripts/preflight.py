"""
Pre-flight check: run this before you present.

    python scripts/preflight.py                 # fast checks (about 20 s)
    python scripts/preflight.py --start-cluster # also start the cluster if it is stopped (waits a few minutes)
    python scripts/preflight.py --deep          # plus: can the cluster reach Event Hubs, does the AI model answer

Prints a checklist and exits non-zero if anything needs fixing. It only reads (and, with
--start-cluster, starts the cluster); it never changes data.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.preflight import (                                  # noqa: E402
    FAIL, INFO, OK, WARN, Check,
    compare_files, deep_check_code, evaluate_cluster, evaluate_deployed_code, evaluate_deep,
    evaluate_environment, evaluate_git, evaluate_jobs, evaluate_secrets, evaluate_warehouse,
    exit_code, format_report, normalize_text, parse_git_status, pinned_models,
)

# What the cluster imports at run time (the same folders the bundle deploys for the jobs).
CODE_DIRS = ("notebooks", "src", "ai_suggester", "config", "rtm_kafka", "configs")
CODE_SUFFIXES = (".py", ".yml")
SECRET_SCOPE = "dqx-poc"


def guarded(name: str, fn) -> list[Check]:
    """Run one check; an unexpected error becomes a [FAIL] line instead of stopping the script."""
    try:
        result = fn()
        return result if isinstance(result, list) else [result]
    except Exception as exc:                                     # noqa: BLE001
        return [Check(name, FAIL, f"{type(exc).__name__}: {str(exc).splitlines()[0][:200] if str(exc) else ''}")]


def local_code_files() -> dict[str, str]:
    files = {}
    for folder in CODE_DIRS:
        for path in sorted((REPO / folder).rglob("*")):
            if path.is_file() and path.suffix in CODE_SUFFIXES and "__pycache__" not in path.parts:
                files[path.relative_to(REPO).as_posix()] = normalize_text(path.read_bytes())
    return files


def read_remote(w, base: str, rel: str) -> str | None:
    """Deployed content of one file; bundle notebooks live at the path without '.py'."""
    from databricks.sdk.service.workspace import ExportFormat
    candidates = [f"{base}/{rel}"] + ([f"{base}/{rel[:-3]}"] if rel.endswith(".py") else [])
    for path in candidates:
        try:
            return normalize_text(b64decode(w.workspace.export(path, format=ExportFormat.SOURCE).content))
        except Exception:                                        # noqa: BLE001 - try the next form
            pass
        try:
            return normalize_text(w.workspace.download(path).read())
        except Exception:                                        # noqa: BLE001
            continue
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-cluster", action="store_true", help="start the cluster if it is stopped")
    parser.add_argument("--deep", action="store_true", help="also test Event Hubs reachability and the AI model")
    args = parser.parse_args()

    from databricks.sdk import WorkspaceClient
    from live_board.data import CLUSTER_ID, PROFILE, WAREHOUSE_ID

    checks: list[Check] = []

    # 1. login ---------------------------------------------------------------
    try:
        w = WorkspaceClient(profile=PROFILE)
        me = w.current_user.me().user_name
        checks.append(Check("Databricks login", OK, f"signed in as {me} (profile {PROFILE})"))
    except Exception as exc:                                     # noqa: BLE001
        checks.append(Check("Databricks login", FAIL,
                            f"{str(exc).splitlines()[0][:160]}. Run: databricks auth login --profile {PROFILE}"))
        print(format_report(checks))
        return 1

    # 2. cluster -------------------------------------------------------------
    started = False
    state = w.clusters.get(CLUSTER_ID).state
    state = state.value if state else None
    if state == "TERMINATED" and args.start_cluster:
        print("Starting the cluster (a few minutes)...", flush=True)
        w.clusters.start_and_wait(CLUSTER_ID)
        state, started = "RUNNING", True
    cluster = evaluate_cluster(state, started)
    checks.append(cluster)
    cluster_up = cluster.status == OK

    # 3. warehouse, secrets, jobs -------------------------------------------
    checks += guarded("SQL warehouse", lambda: evaluate_warehouse(
        (lambda s: s.value if s else None)(w.warehouses.get(WAREHOUSE_ID).state)))
    checks += guarded("Secrets", lambda: evaluate_secrets({s.key for s in w.secrets.list_secrets(SECRET_SCOPE)}, SECRET_SCOPE))
    checks += guarded("Demo jobs", lambda: evaluate_jobs([j.settings.name for j in w.jobs.list() if j.settings and j.settings.name]))

    # 4. deployed code == repo ----------------------------------------------
    def code_check() -> Check:
        base = f"/Workspace/Users/{me}/.bundle/databricks-dqx-poc/dev/files"
        local = local_code_files()
        with ThreadPoolExecutor(max_workers=8) as pool:
            remote = dict(zip(local, pool.map(lambda rel: read_remote(w, base, rel), local)))
        return evaluate_deployed_code(compare_files(local, remote), len(local))
    checks += guarded("Deployed code", code_check)

    # 5. git -----------------------------------------------------------------
    def git_check() -> Check:
        status = parse_git_status(subprocess.run(["git", "status", "--porcelain"], cwd=REPO,
                                                 capture_output=True, text=True, check=True).stdout)
        ahead = subprocess.run(["git", "rev-list", "--count", "new-origin/main..HEAD"], cwd=REPO,
                               capture_output=True, text=True)
        return evaluate_git(status, int(ahead.stdout.strip() or 0) if ahead.returncode == 0 else 0)
    checks += guarded("Git working tree", git_check)

    # 6. board connection + environment state (needs the cluster) -------------
    if cluster_up:
        def board_and_state() -> list[Check]:
            from live_board.data import SqlRunner
            from src.status_view import collect_fast, collect_slow
            from config.constants import TABLE_RULES
            runner = SqlRunner("cluster")
            try:
                runner.query("SELECT 1")
                fast, slow = collect_fast(runner), collect_slow(runner)
                registry = runner.table_exists(TABLE_RULES)
                state = {
                    "registry_rules": len(slow["rules"]) if registry else None,
                    "pending": slow["sugg_counts"].get("pending", 0),
                    "stream_clean": fast["clean"], "stream_quarantine": fast["quarantine"],
                }
                if not registry and fast["clean"] == 0 and fast["quarantine"] == 0:
                    state["stream_clean"] = None
                return [Check("Board can read the tables", OK, f"connected via {runner.http_path}"),
                        evaluate_environment(state)]
            finally:
                runner.close()
        checks += guarded("Board connection", board_and_state)
    else:
        checks.append(Check("Board can read the tables", WARN, "skipped: the cluster is not running"))

    # 7. deep ----------------------------------------------------------------
    if args.deep:
        if not cluster_up:
            checks.append(Check("Deep checks", WARN, "skipped: the cluster is not running"))
        else:
            def deep() -> list[Check]:
                import yaml
                from live_board.cluster_session import ReviewSession, parse_result
                models = pinned_models(yaml.safe_load((REPO / "databricks.yml").read_text(encoding="utf-8")))
                session = ReviewSession()
                try:
                    return evaluate_deep(parse_result(session.run_code(deep_check_code(models))))
                finally:
                    session.close()
            checks += guarded("Deep checks", deep)
    else:
        checks.append(Check("Deep checks", INFO, "not run (add --deep to test Event Hubs and the AI model)"))

    print(format_report(checks))
    return exit_code(checks)


if __name__ == "__main__":
    sys.exit(main())
