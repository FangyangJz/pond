import pytest
import os
import datetime as dtm
from pond.clickhouse.manager import ClickHouseManager

TEST_DB = "quant"


@pytest.fixture(scope="session")
def clickhouse_manager():
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PWD")
    test_db = os.environ.get("CLICKHOUSE_TEST_DB", TEST_DB)
    conn_str = f"clickhouse://{user}:{password}@localhost:8123/{test_db}"
    native_conn_str = f"clickhouse+native://{user}:{password}@localhost:9000/{test_db}?tcp_keepalive=true"
    manager = ClickHouseManager(
        conn_str, data_start=dtm.datetime(2020, 1, 1), native_uri=native_conn_str
    )
    return manager
