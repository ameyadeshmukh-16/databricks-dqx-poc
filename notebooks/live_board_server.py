"""
Live Board Server — serves the Streamlit live board from the cluster itself.

Start this task, then open the link it prints (also shown in the run's logs) in any browser that is
signed in to the workspace; nothing needs to be installed on the presenter's laptop. The board reads the
pipeline's tables through the cluster and runs approvals on it, signed in as the user the job runs as.
The task stays up until the run is stopped or its time cap is reached.

Parameters (spark_python_task argv):
  --port=8501
"""

# -----------------------------------
# Imports
# -----------------------------------
import inspect
import os
import signal
import subprocess
import sys

from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from src.board_launch import DEFAULT_PORT, board_environment, board_url, streamlit_command
from src.logger import get_logger

logger = get_logger(__name__)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


port = int(_arg("port", str(DEFAULT_PORT)))

spark = SparkSession.builder.getOrCreate()
tag = "spark.databricks.clusterUsageTags."
cluster_id = spark.conf.get(tag + "clusterId")
org_id = spark.conf.get(tag + "orgId")
host = spark.conf.get("spark.databricks.workspaceUrl")

url = board_url(host, org_id, cluster_id, port)
logger.info("Live board link (open in a browser signed in to the workspace): %s", url)

from databricks.sdk.core import Config

# The run's own login, read here (where the cluster's runtime auth works) and handed to the board process.
token = Config().authenticate()["Authorization"].split(" ", 1)[1]

app_path = os.path.join(_repo_root, "live_board", "app.py")
process = subprocess.Popen(
    streamlit_command(sys.executable, app_path, port),
    env=board_environment(os.environ, cluster_id, host, token),
    cwd=_repo_root,
)


def _stop(*_):
    process.terminate()


signal.signal(signal.SIGTERM, _stop)
try:
    code = process.wait()
finally:
    if process.poll() is None:
        process.terminate()
logger.warning("The board process ended with exit code %s", code)
sys.exit(code)
