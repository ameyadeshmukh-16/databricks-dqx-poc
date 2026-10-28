"""
Live Monitor — long-running task in the streaming pipeline.

Prints a compact one-screen status view (events in, rules enforced, AI suggestions,
what the reviewer should do next, and what changed since the last snapshot) every
--interval_seconds so the whole loop can be followed from one task's output while
the pipeline runs. Read-only; it ends when the job-level timeout (or a manual
cancel) stops the run.

Parameters (spark_python_task argv):
  --interval_seconds=30
  --run_config_name=streaming_event_rules
  --last_batches=8
"""

# -----------------------------------
# Imports
# -----------------------------------
import inspect
import os
import sys
import time

from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from src.logger import get_logger
from src.status_view import render_snapshot

logger = get_logger(__name__)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


interval_seconds = int(_arg("interval_seconds", "30"))
run_config_name  = _arg("run_config_name", "streaming_event_rules")
last_batches     = int(_arg("last_batches", "8"))

spark = SparkSession.builder.getOrCreate()
logger.info("Live monitor started: every %ds for %s", interval_seconds, run_config_name)

previous = None
while True:
    try:
        # The screen has its own banner and timestamp; passing the previous snapshot
        # adds the "since last snapshot" line.
        previous = render_snapshot(spark, run_config_name=run_config_name,
                                   last_batches=last_batches, prev=previous)
        print("\n", flush=True)
    except Exception as exc:  # noqa: BLE001 - tables may not exist yet; never stop the monitor
        logger.warning("Snapshot failed, will retry: %s", exc)
    sys.stdout.flush()
    time.sleep(interval_seconds)
