"""
Live Monitor — long-running task in the streaming pipeline.

Prints a fresh status snapshot (event flow, active rules, rules firing, pending
suggestions, notifications) every --interval_seconds so the whole loop can be
followed from one task's output while the pipeline runs. Read-only; it ends when
the job-level timeout (or a manual cancel) stops the run.

Parameters (spark_python_task argv):
  --interval_seconds=45
  --run_config_name=streaming_event_rules
  --last_batches=5
"""

# -----------------------------------
# Imports
# -----------------------------------
import inspect
import os
import sys
import time
from datetime import datetime, timezone

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


interval_seconds = int(_arg("interval_seconds", "45"))
run_config_name  = _arg("run_config_name", "streaming_event_rules")
last_batches     = int(_arg("last_batches", "5"))

spark = SparkSession.builder.getOrCreate()
logger.info("Live monitor started: every %ds for %s", interval_seconds, run_config_name)

while True:
    print(f"\n{'#' * 70}\n# SNAPSHOT {datetime.now(tz=timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n{'#' * 70}", flush=True)
    try:
        render_snapshot(spark, run_config_name=run_config_name, last_batches=last_batches)
    except Exception as exc:  # noqa: BLE001 - tables may not exist yet; never stop the monitor
        logger.warning("Snapshot failed, will retry: %s", exc)
    sys.stdout.flush()
    time.sleep(interval_seconds)
