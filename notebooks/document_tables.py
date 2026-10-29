"""
Document Tables — apply plain-English comments to the demo's tables and columns.

The streaming pipeline creates its tables after it starts (the stream's own tables, then the metrics
and suggestion tables as data flows), so this task checks every few seconds and documents each table
once it exists, then ends when all are done. The stream's own tables are left alone until the stream has
completed a few micro-batches, because a metadata change during the stream's first write makes it fail.
It never changes data; running it again is harmless.

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
from src.table_docs import STREAMING_TABLES, apply_table_docs, docs_to_apply

logger = get_logger(__name__)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


interval = int(_arg("interval_seconds", "20"))
timeout = int(_arg("timeout_seconds", "600"))

def _batches_written(spark) -> int:
    """Micro-batches the stream has completed (0 while its tables do not exist yet)."""
    name = f"{CATALOG}.{DATABASE}.stream_batch_metrics"
    try:
        return spark.table(name).count() if spark.catalog.tableExists(name) else 0
    except Exception:                                          # noqa: BLE001 - not readable yet
        return 0


spark = SparkSession.builder.getOrCreate()
deadline = time.time() + timeout
done: set = set()
failed: list = []

while True:
    # Each table is documented once; the stream's own tables only after it has settled (see docs_to_apply).
    result = apply_table_docs(spark, CATALOG, DATABASE, docs_to_apply(done, _batches_written(spark)))
    done.update(result["documented"])
    failed = result["failed"]
    waiting = [t for t in STREAMING_TABLES if t not in done]
    logger.info("Documented %d table(s); still waiting for: %s", len(done), waiting or "nothing")
    if not waiting or time.time() >= deadline:
        break
    time.sleep(interval)

if failed:
    logger.warning("Some comments could not be applied: %s", failed)
if waiting:
    logger.warning("Gave up waiting for: %s", waiting)
else:
    logger.info("All streaming tables are documented.")
