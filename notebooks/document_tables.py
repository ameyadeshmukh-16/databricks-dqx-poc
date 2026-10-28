"""
Document Tables — apply plain-English comments to the demo's tables and columns.

The streaming pipeline creates its tables after it starts (the stream's own tables, then the metrics
and suggestion tables as data flows), so this task re-applies the comments every few seconds until
all of them exist, then ends. It never changes data; running it again is harmless.

Parameters (spark_python_task argv):
  --interval_seconds=20
  --timeout_seconds=600
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

from config.constants import CATALOG, DATABASE
from src.logger import get_logger
from src.table_docs import STREAMING_TABLES, apply_table_docs

logger = get_logger(__name__)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


interval = int(_arg("interval_seconds", "20"))
timeout = int(_arg("timeout_seconds", "600"))

spark = SparkSession.builder.getOrCreate()
deadline = time.time() + timeout
result = apply_table_docs(spark, CATALOG, DATABASE)

while True:
    waiting = [t for t in STREAMING_TABLES if t in result["missing"]]
    logger.info("Documented %d table(s); still waiting for: %s", len(result["documented"]), waiting or "nothing")
    if not waiting or time.time() >= deadline:
        break
    time.sleep(interval)
    result = apply_table_docs(spark, CATALOG, DATABASE)

if result["failed"]:
    logger.warning("Some comments could not be applied: %s", result["failed"])
if waiting:
    logger.warning("Gave up waiting for: %s", waiting)
else:
    logger.info("All streaming tables are documented.")
