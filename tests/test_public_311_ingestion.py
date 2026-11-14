"""Network-free tests for source allowlisting, deterministic pages and manifests."""
import json
from urllib.parse import parse_qs, urlparse
import pytest
from scripts.ingest_public_311 import ingest, request_json, PUBLIC_FIELDS


def test_bounded_page_manifest_and_privacy(tmp_path):
    calls = []
    def fetch(url, *, headers):
        calls.append(url)
        if "open-meteo" in url:
            return {"daily": {"time": ["2025-01-01"],
                              "temperature_2m_mean": [4.2],
                              "precipitation_sum": [0.0]}}
        offset = int(parse_qs(urlparse(url).query)["$offset"][0])
        if offset:
            return []
        return [{"unique_key": "42", "created_date": "2025-01-01T12:00:00",
                 "agency": "DOT", "status": "Closed",
                 "incident_address": "Must never be stored", "latitude": "40.7"}]
    output = ingest("2025-01-01", "2025-01-02", tmp_path,
                    page_size=1, max_pages=3, fetch=fetch)
    assert output["total_311_rows"] == 1
    assert len(calls) == 3
    payload = next((tmp_path / "nyc311").glob("*.jsonl")).read_text()
    assert "incident_address" not in payload and "latitude" not in payload
    assert set(json.loads(payload)) == set(PUBLIC_FIELDS)
    assert output["files"][0]["sha256"] and output["files"][0]["rows"] == 1
    assert not output["partial_window_possible"]


def test_no_unbounded_download_or_arbitrary_host(tmp_path):
    with pytest.raises(ValueError):
        ingest("2025-01-01", "2025-03-01", tmp_path, with_weather=False, fetch=lambda *a, **k: [])
    with pytest.raises(ValueError):
        ingest("2025-01-01", "2025-01-02", tmp_path,
               page_size=99999, with_weather=False, fetch=lambda *a, **k: [])
    with pytest.raises(ValueError):
        request_json("https://example.com/private")


def test_replay_produces_same_file_hashes(tmp_path):
    def fake(url, *, headers):
        return [{"unique_key": "7", "created_date": "2025-01-01T01:00:00"}]
    args = ("2025-01-01", "2025-01-02", tmp_path)
    first = ingest(*args, page_size=2, max_pages=1, with_weather=False, fetch=fake)
    second = ingest(*args, page_size=2, max_pages=1, with_weather=False, fetch=fake)
    assert first == second
