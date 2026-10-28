"""
Streaming Rule Suggester — long-running task in the streaming job.

Runs alongside stream_and_validate and the producer. It waits for the streaming
quarantine table to fill, then every --interval_seconds looks at the new bad
events and the clean data, stages NEW rule suggestions in dq_rules_staging
(pending human review) and records a row in dq_review_notifications.

It never terminates on its own; the job-level timeout (or a manual cancel) stops it.

Parameters (spark_python_task argv):
  --interval_seconds=90     pause between cycles
  --min_rows=60             quarantined rows required before the first cycle
  --min_new_rows=30         new quarantined rows required for later cycles
  --max_new=5               max new suggestions staged per cycle
  --llm_backend=openrouter  openrouter | claude | azure_openai
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

from ai_suggester.suggestion_cycle import run_cycle, should_run_cycle
from config.constants import (
    STREAMING_RULE_CONFIG_NAME,
    TABLE_STREAM_CLEAN,
    TABLE_STREAM_QUARANTINE,
)
from src.logger import get_logger

logger = get_logger(__name__)

CONTEXT = (
    "Retail POS events streamed from stores over Kafka (Azure Event Hubs): "
    "event_id, store_id, product_id, division, quantity, unit_price, coupon_applied, event_timestamp"
)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


interval_seconds = int(_arg("interval_seconds", "90"))
min_rows         = int(_arg("min_rows", "60"))
min_new_rows     = int(_arg("min_new_rows", "30"))
max_new          = int(_arg("max_new", "5"))
llm_backend      = _arg("llm_backend", "openrouter")

spark = SparkSession.builder.getOrCreate()

logger.info("Streaming rule suggester started: interval=%ds min_rows=%d min_new_rows=%d max_new=%d backend=%s",
            interval_seconds, min_rows, min_new_rows, max_new, llm_backend)


def _quarantine_count() -> int:
    if not spark.catalog.tableExists(TABLE_STREAM_QUARANTINE):
        return 0
    return spark.table(TABLE_STREAM_QUARANTINE).count()


# -----------------------------------
# Loop (runs until the job is stopped)
# -----------------------------------
last_count = None
while True:
    try:
        count = _quarantine_count()
        if should_run_cycle(count, last_count, min_rows, min_new_rows):
            logger.info("Suggestion cycle starting: %d quarantined rows (last cycle saw %s)", count, last_count)
            run_cycle(
                spark,
                quarantine_table=TABLE_STREAM_QUARANTINE,
                profile_table=TABLE_STREAM_CLEAN,
                run_config_name=STREAMING_RULE_CONFIG_NAME,
                context=CONTEXT,
                llm_backend=llm_backend,
                max_new=max_new,
            )
            last_count = count
        else:
            logger.info("Waiting: %d quarantined rows (last cycle saw %s)", count, last_count)
    except Exception as exc:  # noqa: BLE001 - keep the loop alive across any transient failure
        logger.warning("Cycle error, will retry next interval: %s", exc)

    time.sleep(interval_seconds)
