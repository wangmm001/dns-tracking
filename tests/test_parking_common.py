# tests/test_parking_common.py
import pytest
from scripts.parking_common import apex_of, parse_snap_tag, snap_url


class TestApexOf:
    @pytest.mark.parametrize("inp,expected", [
        ("example.com",               "example.com"),
        ("www.example.com",           "example.com"),
        ("a.b.c.example.com",         "example.com"),
        ("EXAMPLE.COM",               "example.com"),
        ("xn--80ak6aa92e.com",        "xn--80ak6aa92e.com"),
        ("foo.io",                    "foo.io"),
        ("",                          ""),
        ("singlelabel",               "singlelabel"),
    ])
    def test_last_two_labels_heuristic(self, inp, expected):
        assert apex_of(inp) == expected

    def test_trailing_dot_stripped(self):
        assert apex_of("www.example.com.") == "example.com"


class TestParseSnapTag:
    @pytest.mark.parametrize("inp,expected", [
        ("snap-2026-05-28-00", ("2026-05-28", "00")),
        ("snap-2026-05-28-12", ("2026-05-28", "12")),
    ])
    def test_valid_tags(self, inp, expected):
        assert parse_snap_tag(inp) == expected

    @pytest.mark.parametrize("bad", [
        "snap-2026-05-28",          # legacy, no HH
        "snap-2026-5-28-00",        # not zero-padded
        "snap-2026-05-28-06",       # invalid hour
        "parking-DAY-2026-05-28-00",
        "",
    ])
    def test_invalid_tags(self, bad):
        with pytest.raises(ValueError):
            parse_snap_tag(bad)


class TestSnapUrl:
    def test_format(self):
        url = snap_url("snap-2026-05-28-12",
                       "newly_registered_domains_measurements", 3,
                       repo="wangmm001/dns-tracking")
        assert url == (
            "https://github.com/wangmm001/dns-tracking/releases/download/"
            "snap-2026-05-28-12/"
            "newly_registered_domains_measurements.shard-3.parquet"
        )
