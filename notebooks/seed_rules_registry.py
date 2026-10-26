"""
Seed Rules Registry — load configs/rules_*.yml into dq_rules_registry.

Converts the YAML rule sets to DQX-native checks, validates them with DQX and
saves them under their run_config_name. A rule set that already has rows in the
registry is skipped, so rules promoted from the AI suggester are never
overwritten; pass force=true to replace a rule set from the YAML files.
"""

# -----------------------------------
# Imports
# -----------------------------------
import os
import sys
import inspect

import yaml
from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from config.constants import (
    TABLE_RULES,
    BATCH_RULE_CONFIG_NAME,
    STREAMING_RULE_CONFIG_NAME,
)
from src.logger import get_logger
from src.rule_seed import yaml_rules_to_dqx_checks

logger = get_logger(__name__)

# -----------------------------------
# Parameters
# -----------------------------------
# spark_python_task passes parameters as argv (--force=true)
force = any(a.lower() == "--force=true" for a in sys.argv[1:])

# -----------------------------------
# Setup
# -----------------------------------
spark = SparkSession.builder.getOrCreate()
dqx = DQEngine(WorkspaceClient())

RULE_SETS = {
    BATCH_RULE_CONFIG_NAME:     os.path.join(_repo_root, "configs", "rules_batch.yml"),
    STREAMING_RULE_CONFIG_NAME: os.path.join(_repo_root, "configs", "rules_streaming.yml"),
}


def _existing_rule_count(run_config_name: str) -> int:
    if not spark.catalog.tableExists(TABLE_RULES):
        return 0
    return spark.table(TABLE_RULES).filter(f"run_config_name = '{run_config_name}'").count()


# -----------------------------------
# Seed
# -----------------------------------
for run_config_name, yaml_path in RULE_SETS.items():
    existing = _existing_rule_count(run_config_name)
    if existing and not force:
        logger.info("'%s' already has %d rules — skipped (force=false)", run_config_name, existing)
        continue

    with open(yaml_path, "r", encoding="utf-8") as f:
        checks = yaml_rules_to_dqx_checks(yaml.safe_load(f))

    status = dqx.validate_checks(checks)
    if status.has_errors:
        raise ValueError(f"Invalid checks for '{run_config_name}': {status.errors}")

    dqx.save_checks(
        checks,
        config=TableChecksStorageConfig(
            location=TABLE_RULES,
            run_config_name=run_config_name,
            mode="overwrite",
        ),
    )
    logger.info("Seeded %d rules into %s for '%s'", len(checks), TABLE_RULES, run_config_name)
