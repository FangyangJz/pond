"""Offline regression checks for archive selection and timestamp conversion."""
import datetime as dt
import importlib.util
import io
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location(
    "backfill_all", Path(__file__).parents[1] / "pond/duckdb/crypto/scripts/backfill_all.py")
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)


def zipped(text):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("test.csv", text)
    return out.getvalue()


class BackfillTest(unittest.TestCase):
    def test_spot_excludes_symbols_absent_from_um_even_when_discovered_first(self):
        def discovery(args, market, kind):
            return (["BTCUSDT", "OLDUSDT", "1000PEPEUSDT"] if market == "um"
                    else ["BTCUSDT", "OLDUSDT", "SPOTONLYUSDT", "PEPEUSDT"])
        with patch.object(b, "discover", side_effect=discovery):
            cache = {}
            self.assertEqual(b.scoped_symbols(None, "spot", "klines", cache),
                             ["BTCUSDT", "OLDUSDT"])
            self.assertIn("1000PEPEUSDT", cache[("um", "klines")])

    def test_spot_microseconds_and_zero_volume_are_preserved(self):
        row = "1735689600000000,1,2,0.5,1,0,1735693199999999,0,0,0,0,0\n"
        f = b.parse_archive(zipped(row), "klines", "BTCUSDT")
        self.assertEqual(f["open_time"][0], dt.datetime(2025, 1, 1))
        self.assertEqual(f["close_time"][0].microsecond, 999999)
        self.assertEqual(f["volume"][0], 0)

    def test_um_header_and_milliseconds(self):
        row = ",".join(b.KCOLS) + "\n1735689600000,1,2,0.5,1,0,1735693199999,0,0,0,0,0\n"
        f = b.parse_archive(zipped(row), "klines", "BTCUSDT")
        self.assertEqual(f["open_time"][0], dt.datetime(2025, 1, 1))
        self.assertEqual(f["close_time"][0].microsecond, 999000)

    def test_daily_fallback_for_interior_month(self):
        monthly = [f"monthly/BTCUSDT-1d-2025-{m}.zip" for m in ["01", "03"]]
        daily = [f"daily/BTCUSDT-1d-2025-{m}-01.zip" for m in ["01", "02", "03", "04"]]
        selected = b.choose_archives(monthly, daily, "BTCUSDT", "klines", "1d",
                                     dt.date(2025, 1, 1), dt.date(2025, 4, 1))
        self.assertEqual(set(selected), set(monthly + [daily[1]]))

    def test_funding_interval_is_not_forced_to_eight(self):
        f = b.parse_archive(zipped("calc_time,funding_interval_hours,last_funding_rate\n1735689600000,4,0.001\n"),
                            "fundingRate", "BTCUSDT")
        self.assertEqual(f["funding_interval_hours"][0], 4)

    def test_metrics_native_grid(self):
        row = ",".join(b.MCOLS) + "\n2025-01-01 00:05:00,BTCUSDT,1,2,3,4,5,6\n"
        f = b.parse_archive(zipped(row), "metrics", "BTCUSDT")
        self.assertEqual(f["create_time"][0], dt.datetime(2025, 1, 1, 0, 5))
        self.assertEqual(f["jj_code"][0], "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
