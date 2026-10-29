"""
Pure helpers for running the live board on the Databricks cluster.

The cluster's driver proxy publishes a port of the driver at
https://<workspace>/driver-proxy/o/<org id>/<cluster id>/<port>/ (workspace login required).
The proxy strips that prefix before forwarding, so Streamlit needs no base path.
"""

from __future__ import annotations

DEFAULT_PORT = 8501


def board_url(workspace_host: str, org_id: str, cluster_id: str, port: int = DEFAULT_PORT) -> str:
    """Browser link to the board, from the values the cluster reports about itself."""
    host = workspace_host.replace("https://", "").replace("http://", "").strip("/")
    return f"https://{host}/driver-proxy/o/{org_id}/{cluster_id}/{port}/"


def streamlit_command(python: str, app_path: str, port: int = DEFAULT_PORT) -> list[str]:
    """Command line that serves the board headless behind the driver proxy."""
    return [
        python, "-m", "streamlit", "run", app_path,
        "--server.port", str(port),
        "--server.headless", "true",
        "--server.enableCORS", "false",
        "--server.enableXsrfProtection", "false",   # the proxy changes the origin; access is gated by the workspace login
        "--server.fileWatcherType", "none",
        "--browser.gatherUsageStats", "false",
    ]


def board_environment(base_env: dict, cluster_id: str, host: str, token: str) -> dict:
    """
    Environment for the board process: no CLI profile, this cluster for queries, and the run's own
    credentials handed over explicitly (the cluster's built-in login only works in the job process itself).
    """
    env = dict(base_env)
    env["BOARD_PROFILE"] = ""
    env["BOARD_CLUSTER_ID"] = cluster_id
    env["DATABRICKS_HOST"] = host if host.startswith("http") else f"https://{host}"
    env["DATABRICKS_TOKEN"] = token
    return env
