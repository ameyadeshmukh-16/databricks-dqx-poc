"""
Reset Demo — wipe the demo environment so the next run starts from empty.

DESTRUCTIVE, so it is a DRY RUN unless the job parameter confirm is exactly RESET:
  Jobs > [RTM] Reset Demo > Run now with different parameters > confirm = RESET

It drops the demo schema (all its tables) and removes the streaming checkpoint. It refuses to run
against a catalog whose name does not end in _dev, and refuses while any other [RTM] job is running.
Seeding is not done here: the streaming pipeline's first task recreates the schema and seeds the rules.

Parameters (spark_python_task argv):
  --confirm=RESET
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

from config.constants import CATALOG, CHECKPOINT_KAFKA, DATABASE
from src.logger import get_logger
from src.reset_plan import ResetRefused, assert_dev_catalog, assert_nothing_running, build_plan, describe_plan

logger = get_logger(__name__)

SCHEMA = f"{CATALOG}.{DATABASE}"
STORAGE_ACCOUNT_KEY_CONF = "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net"


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


confirm = _arg("confirm", "")
spark = SparkSession.builder.getOrCreate()
spark.conf.set(STORAGE_ACCOUNT_KEY_CONF, os.environ["AZURE_STORAGE_KEY"])      # same access the pipeline uses

# dbutils is injected by Databricks in jobs
fs = dbutils.fs  # noqa: F821


def checkpoint_exists(path: str) -> bool:
    try:
        fs.ls(path)
        return True
    except Exception:                                   # noqa: BLE001 - a missing folder raises
        return False


# -----------------------------------
# Safety rails (these stop the job with a clear message)
# -----------------------------------
assert_dev_catalog(CATALOG)

from databricks.sdk import WorkspaceClient
active = [r.run_name or "" for r in WorkspaceClient().jobs.list_runs(active_only=True)]
assert_nothing_running(active)

# -----------------------------------
# What is there
# -----------------------------------
tables: dict = {}
if spark.catalog.databaseExists(SCHEMA):
    for row in spark.sql(f"SHOW TABLES IN {SCHEMA}").collect():
        name = row["tableName"]
        try:
            tables[name] = spark.table(f"{SCHEMA}.{name}").count()
        except Exception:                               # noqa: BLE001 - still list it, just without a count
            tables[name] = None
checkpoints = [CHECKPOINT_KAFKA] if checkpoint_exists(CHECKPOINT_KAFKA) else []

plan = build_plan(SCHEMA, tables, checkpoints, confirm)
print(describe_plan(plan))

if plan["mode"] == "execute":
    # -------------------------------
    # Execute
    # -------------------------------
    if tables or spark.catalog.databaseExists(SCHEMA):
        spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    for path in checkpoints:
        fs.rm(path, True)

    # -------------------------------
    # Verify
    # -------------------------------
    leftovers = []
    if spark.catalog.databaseExists(SCHEMA):
        leftovers.append(f"schema {SCHEMA}")
    leftovers += [p for p in checkpoints if checkpoint_exists(p)]
    if leftovers:
        raise RuntimeError("Reset incomplete, still present: " + ", ".join(leftovers))
    print("DONE: schema dropped and checkpoint removed. The next pipeline run starts from empty.")
# (a dry run simply ends here; exiting with sys.exit would make Databricks report the task as failed)
