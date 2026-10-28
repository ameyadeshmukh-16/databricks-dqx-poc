"""
Status Snapshot — a one-click, read-only view of the streaming loop.

Run from the Jobs UI: "[RTM] Live Status" > Run now. Reads the output; changes nothing.
(Inside the streaming pipeline the same view refreshes continuously via live_monitor.py.)

Parameters (spark_python_task argv):
  --run_config_name=streaming_event_rules
  --last_batches=5
"""

# -----------------------------------
# Imports
# -----------------------------------
import inspect
import os
import sys

from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from src.status_view import render_snapshot


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


render_snapshot(
    SparkSession.builder.getOrCreate(),
    run_config_name=_arg("run_config_name", "streaming_event_rules"),
    last_batches=int(_arg("last_batches", "5")),
)
