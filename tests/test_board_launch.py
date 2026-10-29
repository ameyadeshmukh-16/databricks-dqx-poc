from src.board_launch import board_environment, board_url, streamlit_command


def test_board_url_from_cluster_facts():
    assert board_url("adb-1.2.azuredatabricks.net", "111", "0101-abc", 8501) == \
        "https://adb-1.2.azuredatabricks.net/driver-proxy/o/111/0101-abc/8501/"


def test_board_url_tolerates_scheme_and_trailing_slash():
    assert board_url("https://adb-1.2.azuredatabricks.net/", "111", "c", 9000).startswith(
        "https://adb-1.2.azuredatabricks.net/driver-proxy/o/111/c/9000/")


def test_streamlit_command_is_headless_with_port():
    cmd = streamlit_command("py", "/x/app.py", 8600)
    assert cmd[:5] == ["py", "-m", "streamlit", "run", "/x/app.py"]
    assert cmd[cmd.index("--server.port") + 1] == "8600"
    assert cmd[cmd.index("--server.headless") + 1] == "true"


def test_board_environment_clears_profile_and_hands_over_credentials_without_mutating_input():
    base = {"BOARD_PROFILE": "dqx-poc-dev", "PATH": "/bin"}
    env = board_environment(base, "0101-abc", "adb-1.2.azuredatabricks.net", "tok")
    assert env["BOARD_PROFILE"] == ""
    assert env["BOARD_CLUSTER_ID"] == "0101-abc"
    assert env["DATABRICKS_HOST"] == "https://adb-1.2.azuredatabricks.net"
    assert env["DATABRICKS_TOKEN"] == "tok"
    assert env["PATH"] == "/bin"
    assert base["BOARD_PROFILE"] == "dqx-poc-dev"
