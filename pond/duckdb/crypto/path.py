from pathlib import Path
from pond.binance_history.type import AssetType, DataType
from pond.duckdb.crypto.const import timeframe_data_types_dict


class CryptoPath:
    def __init__(self, crypto_path: Path) -> None:
        self.crypto = crypto_path
        self.data = self.crypto / "data"
        self.info = self.crypto / "info"

        # kline data path
        self.kline = self.crypto / "kline"
        self.kline_spot = self.kline / "spot"
        self.kline_um = self.kline / "um"
        self.kline_cm = self.kline / "cm"

        # trades data path
        self.trades = self.crypto / "trades"
        self.trades_origin = self.trades / "origin"
        self.trades_spot = self.trades / "spot"
        self.trades_um = self.trades / "um"
        self.trades_cm = self.trades / "cm"

        # aggTrades data path
        self.aggTrades = self.crypto / "agg_trades"
        self.aggTrades_origin = self.aggTrades / "origin"
        self.aggTrades_spot = self.aggTrades / "spot"
        self.aggTrades_um = self.aggTrades / "um"
        self.aggTrades_cm = self.aggTrades / "cm"

        # metrics data path
        self.metrics = self.crypto / "metrics"
        self.metrics_um = self.metrics / "um"
        self.metrics_cm = self.metrics / "cm"

        # fundingRate data path
        self.fundingRate = self.crypto / "funding_rate"
        self.fundingRate_um = self.fundingRate / "um"
        self.fundingRate_cm = self.fundingRate / "cm"

        #  orderbook data path
        self.orderbook = self.crypto / "orderbook"

        self.path_list = [
            self.crypto,
            self.data,
            self.info,
            # kline path
            self.kline,
            self.kline_spot,
            *self.get_common_interval_path_list(self.kline_spot),
            self.kline_um,
            *self.get_common_interval_path_list(self.kline_um),
            self.kline_cm,
            *self.get_common_interval_path_list(self.kline_cm),
            # trades path
            self.trades,
            self.trades_origin,
            self.trades_spot,
            self.trades_um,
            self.trades_cm,
            # aggtrades path
            self.aggTrades,
            self.aggTrades_origin,
            self.aggTrades_spot,
            self.aggTrades_um,
            self.aggTrades_cm,
            # metrics path
            self.metrics,
            self.metrics_cm,
            self.metrics_cm / "1d",
            self.metrics_um,
            self.metrics_um / "1d",
            # fundingRate path
            self.fundingRate,
            self.fundingRate_cm,
            self.fundingRate_cm / "1M",
            self.fundingRate_um,
            self.fundingRate_um / "1M",
            # orderbook path
            self.orderbook,
        ]

    @staticmethod
    def get_common_interval_path_list(base_path: Path):
        return [base_path / freq for freq in timeframe_data_types_dict.keys()]

    def init_db_path(self):
        [f.mkdir() for f in self.path_list if not f.exists()]

    def get_base_path(self, asset_type: AssetType, data_type: DataType) -> Path:
        return {
            # kline path map
            (AssetType.spot, DataType.klines): self.kline_spot,
            (AssetType.future_cm, DataType.klines): self.kline_cm,
            (AssetType.future_um, DataType.klines): self.kline_um,
            # trades path map
            (AssetType.spot, DataType.trades): self.trades_spot,
            (AssetType.future_cm, DataType.trades): self.trades_cm,
            (AssetType.future_um, DataType.trades): self.trades_um,
            # aggtrades path map
            (AssetType.spot, DataType.aggTrades): self.aggTrades_spot,
            (AssetType.future_cm, DataType.aggTrades): self.aggTrades_cm,
            (AssetType.future_um, DataType.aggTrades): self.aggTrades_um,
            # metrics path map
            (AssetType.future_um, DataType.metrics): self.metrics_um,
            (AssetType.future_cm, DataType.metrics): self.metrics_cm,
            # fundingRate path map
            (
                AssetType.future_um,
                DataType.fundingRate,
            ): self.fundingRate_um,
            (
                AssetType.future_cm,
                DataType.fundingRate,
            ): self.fundingRate_cm,
        }[(asset_type, data_type)]  # type: ignore
