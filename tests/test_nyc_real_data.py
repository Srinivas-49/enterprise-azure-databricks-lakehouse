"""Offline source emulation tests. No requests to NYC services in CI."""
import json
from urllib.parse import parse_qs, urlparse
import pytest
from real_data.nyc_sources import fetch_source, aggregate, month, borough_name, read_landed

def test_two_real_source_contracts_and_aggregation(tmp_path):
    def mock_fetch(url):
        q = parse_qs(urlparse(url).query)
        assert q["$limit"] == ["2"] and q["$offset"] == ["0"]
        if "erm2-nwe9" in url:
            return [{"unique_key": "1", "created_date": "2025-06-05T11:00:00",
                     "borough": "BROOKLYN", "agency": "DOT"},
                    {"unique_key": "2", "created_date": "2025-06-06T08:00:00",
                     "borough": "BROOKLYN", "agency": "NYPD"}]
        return [{"camis": "CA", "inspection_date": "2025-06-06T00:00:00",
                 "boro": "3", "critical_flag": "Critical"},
                {"camis": "CA", "inspection_date": "2025-06-06T00:00:00",
                 "boro": "3", "critical_flag": "Not Critical"}]
    for source in ("service_requests", "restaurant_inspections"):
        m = fetch_source(source, tmp_path, page_size=2, pages=1, fetch=mock_fetch)
        assert m["records_landed"] == 2
    result = aggregate(tmp_path)
    assert result["rows"] == [{"borough": "BROOKLYN", "month": "2025-06",
                               "service_requests": 2, "distinct_restaurant_inspections": 1,
                               "critical_violation_citations": 1}]
    with pytest.raises(ValueError, match="checksum"):
        p = tmp_path / "service_requests" / "page_000000.jsonl"
        p.write_text("corrupt")
        list(read_landed(tmp_path, "service_requests"))

def test_safe_borough_and_dates():
    assert borough_name("2") == "BRONX"
    assert borough_name("unknown") is None
    assert month("2025-13-01") is None

def test_page_limits_and_resume(tmp_path):
    calls = []
    def fetch(url):
        calls.append(url)
        return [{"unique_key": "5", "created_date": "2025-06-01",
                 "borough": "QUEENS"}]
    a = fetch_source("service_requests", tmp_path, page_size=10, pages=2, fetch=fetch)
    assert a["records_landed"] == 1
    b = fetch_source("service_requests", tmp_path, page_size=10, pages=2, fetch=fetch)
    assert b["records_landed"] == 1 and len(calls) == 1
    with pytest.raises(ValueError):
        fetch_source("service_requests", tmp_path, pages=1001)
