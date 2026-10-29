"""
Warm review session on the cluster.

Approving a rule needs DQX validation, which needs a Spark session, so it cannot run
on the presenter's laptop. This keeps ONE Python session open on the (already running)
cluster and runs `src.review_service.run_review` in it, so an Approve / Reject click
takes seconds instead of the ~1 minute a job needs to start. It adds no compute: it is
an idle Python process on the cluster that is up anyway, and it is closed when the
board pauses.
"""

from __future__ import annotations

import json
import threading

from live_board.data import CLUSTER_ID, PROFILE

MARKER = "@@RESULT@@"
VALID_ACTIONS = ("list", "approve", "reject")


class ReviewSessionError(RuntimeError):
    """The cluster session failed (not a refused review — those come back as results)."""


def build_command(action: str, rule_ref: str, run_config_name: str) -> str:
    """Python source run on the cluster; prints one marked JSON line."""
    if action not in VALID_ACTIONS:
        raise ValueError(f"unknown action {action!r}")
    return (
        "import json\n"
        "from src.review_service import run_review\n"
        f"_res = run_review(spark, {action!r}, {str(rule_ref)!r}, {str(run_config_name)!r})\n"
        f"print({MARKER!r} + json.dumps(_res, default=str))\n"
    )


def parse_result(output: str) -> dict:
    """Pull the marked JSON line out of the command output."""
    for line in reversed((output or "").splitlines()):
        if line.startswith(MARKER):
            return json.loads(line[len(MARKER):])
    raise ReviewSessionError("The cluster did not return a review result.")


class ReviewSession:
    def __init__(self, profile: str = PROFILE, cluster_id: str = CLUSTER_ID, client=None,
                 bundle_path: str | None = None):
        self._profile = profile
        self._cluster_id = cluster_id
        self._client = client
        self._bundle_path = bundle_path
        self._context_id: str | None = None
        self._lock = threading.Lock()

    # -- plumbing ---------------------------------------------------------
    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(profile=self._profile or None)   # empty = the environment's own login
        return self._client

    @property
    def bundle_path(self) -> str:
        if self._bundle_path is None:
            user = self.client.current_user.me().user_name
            self._bundle_path = f"/Workspace/Users/{user}/.bundle/databricks-dqx-poc/dev/files"
        return self._bundle_path

    @staticmethod
    def _language():
        from databricks.sdk.service.compute import Language
        return Language.PYTHON

    def _run(self, code: str) -> str:
        response = self.client.command_execution.execute_and_wait(
            cluster_id=self._cluster_id, context_id=self._context_id,
            language=self._language(), command=code,
        )
        results = getattr(response, "results", None)
        if results is None:
            raise ReviewSessionError("The cluster returned no result.")
        kind = getattr(results.result_type, "value", results.result_type)
        if str(kind).lower() == "error":
            raise ReviewSessionError(str(getattr(results, "cause", None) or "command failed").splitlines()[-1][:300])
        return getattr(results, "data", "") or ""

    def _ensure(self) -> None:
        if self._context_id:
            return
        ctx = self.client.command_execution.create_and_wait(cluster_id=self._cluster_id, language=self._language())
        self._context_id = ctx.id
        self._run(
            "import sys\n"
            f"sys.path.insert(0, {self.bundle_path!r})\n"
            "import src.review_service\n"
            "from databricks.labs.dqx.engine import DQEngine   # warm the heavy import now\n"
        )

    def _reset(self) -> None:
        context_id, self._context_id = self._context_id, None
        if context_id:
            try:
                self.client.command_execution.destroy(cluster_id=self._cluster_id, context_id=context_id)
            except Exception:                                   # noqa: BLE001 - best effort
                pass

    # -- public -----------------------------------------------------------
    def warm_up(self) -> None:
        with self._lock:
            self._ensure()

    def review(self, action: str, rule_ref: str = "", run_config_name: str = "streaming_event_rules") -> dict:
        """Run one review action on the cluster; re-creates the session once if it was lost."""
        command = build_command(action, rule_ref, run_config_name)
        with self._lock:
            for attempt in (1, 2):
                try:
                    self._ensure()
                    return parse_result(self._run(command))
                except Exception as exc:                       # noqa: BLE001
                    self._reset()
                    if attempt == 2:
                        raise ReviewSessionError(str(exc).splitlines()[0][:300]) from exc
        raise ReviewSessionError("unreachable")                # pragma: no cover

    def run_code(self, code: str) -> str:
        """Run arbitrary Python in the warm session and return its printed output (used by the pre-flight check)."""
        with self._lock:
            for attempt in (1, 2):
                try:
                    self._ensure()
                    return self._run(code)
                except Exception as exc:                       # noqa: BLE001
                    self._reset()
                    if attempt == 2:
                        raise ReviewSessionError(str(exc).splitlines()[0][:300]) from exc
        raise ReviewSessionError("unreachable")                # pragma: no cover

    def close(self) -> None:
        with self._lock:
            self._reset()
