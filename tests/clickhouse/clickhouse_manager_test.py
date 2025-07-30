from datetime import datetime
from pond.clickhouse.manager import ClickHouseManager


def test_read_latest_n_record(clickhouse_manager: ClickHouseManager):
    start = datetime(2024, 1, 1)
    end = datetime.now()
    n = 1
    df = clickhouse_manager.read_latest_n_record("future_info", start, end, n=n)
    assert df is not None
    assert not df.empty
    assert df["code"].unique().size > 1
    assert (df["row_n"] <= n).all()
