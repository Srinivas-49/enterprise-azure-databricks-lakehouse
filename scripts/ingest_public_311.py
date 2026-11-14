"""Bounded public NYC 311 + Open-Meteo ingestion with audit manifests.

Examples:
  python -m scripts.ingest_public_311 --start 2025-01-01 --end 2025-01-03 --out raw-public
  python -m scripts.ingest_public_311 --start 2025-01-01 --end 2025-02-01 --max-pages 30

Never collects street addresses, coordinates, caller details or raw free text.
A batch is not a consistent snapshot of the mutable 311 dataset.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import time
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

NYC_311 = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
OPEN_METEO = "https://archive-api.open-meteo.com/v1/archive"
PUBLIC_FIELDS = ("unique_key", "created_date", "closed_date", "agency",
                 "complaint_type", "borough", "status")
ALLOWED_HOSTS = ("data.cityofnewyork.us", "archive-api.open-meteo.com")


def request_json(url, *, headers=None, retries=3):
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError("Only explicitly approved public-data HTTPS hosts may be requested")
    headers = {"User-Agent": "portfolio-public-data-ingestion/1.0", **(headers or {})}
    for attempt in range(retries):
        try:
            with urlopen(Request(url, headers=headers), timeout=35) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise
            delay = exc.headers.get("Retry-After", "") if exc.headers else ""
            time.sleep(min(10, float(delay)) if delay.isdigit() else 1 + attempt)
        except (TimeoutError, URLError):
            if attempt == retries - 1:
                raise
            time.sleep(1 + attempt)


def write_jsonl(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    digest = hashlib.sha256()
    with tmp.open("wb") as output:
        for item in records:
            line = (json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            digest.update(line)
            output.write(line)
    os.replace(tmp, path)
    return {"path": str(path), "rows": len(records), "sha256": digest.hexdigest(),
            "bytes": path.stat().st_size}


def ingest(start, end, out_dir, *, page_size=1000, max_pages=2,
           with_weather=True, fetch=request_json):
    """One bounded date window; loop over windows rather than unlimited API crawling."""
    begin, finish = date.fromisoformat(start), date.fromisoformat(end)
    if not begin < finish or (finish - begin).days > 31:
        raise ValueError("Require date interval of 1 to 31 days, end exclusive")
    if not 1 <= page_size <= 5000 or not 1 <= max_pages <= 100:
        raise ValueError("page_size must be <=5000; max_pages <=100")
    root = Path(out_dir)
    where = (f"created_date >= '{start}T00:00:00' "
             f"AND created_date < '{end}T00:00:00'")
    files = []
    count = 0
    for page in range(max_pages):
        query = urlencode({
            "$select": ",".join(PUBLIC_FIELDS),
            "$where": where,
            "$order": "created_date ASC, unique_key ASC",
            "$limit": page_size,
            "$offset": page * page_size,
        })
        api_headers = {}
        token = os.environ.get("SOCRATA_APP_TOKEN")
        if token:
            api_headers["X-App-Token"] = token  # Never persisted to disk.
        rows = fetch(NYC_311 + "?" + query, headers=api_headers)
        if not isinstance(rows, list):
            raise ValueError("311 response must be a JSON list")
        # Allowlist deliberately excludes location, address and free-text fields.
        cleaned = [{k: row.get(k) for k in PUBLIC_FIELDS} for row in rows]
        if cleaned:
            files.append(write_jsonl(
                root / "nyc311" / f"requests_{start}_{end}_p{page:03d}.jsonl", cleaned))
            count += len(cleaned)
        if len(rows) < page_size:
            break

    if with_weather:
        query = urlencode({"latitude": 40.7128, "longitude": -74.0060,
                           "start_date": start,
                           "end_date": (finish.fromordinal(finish.toordinal() - 1)).isoformat(),
                           "daily": "temperature_2m_mean,precipitation_sum",
                           "timezone": "America/New_York"})
        weather = fetch(OPEN_METEO + "?" + query, headers={})
        daily = weather.get("daily", {})
        days = daily.get("time", [])
        temps = daily.get("temperature_2m_mean", [])
        rain = daily.get("precipitation_sum", [])
        if not len(days) == len(temps) == len(rain):
            raise ValueError("Weather daily arrays do not have matching lengths")
        records = [{"date": day, "temperature_mean_c": temp, "precip_mm": mm,
                    "city": "NYC", "source": "open-meteo"}
                   for day, temp, mm in zip(days, temps, rain)]
        files.append(write_jsonl(root / "weather" / f"daily_{start}_{end}.jsonl", records))
    manifest = {
        "sources": ["NYC Open Data 311", "Open-Meteo historical weather"] if with_weather
                   else ["NYC Open Data 311"],
        "dataset": "erm2-nwe9", "window_start": start, "window_end_exclusive": end,
        "total_311_rows": count, "max_pages": max_pages,
        "page_size": page_size, "partial_window_possible": count == page_size * max_pages,
        "contains_personal_addresses": False,
        "files": files,
        "source_url": "https://data.cityofnewyork.us/d/erm2-nwe9",
        "weather_attribution": "Weather data by Open-Meteo.com (CC BY 4.0)",
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / f"manifest_{start}_{end}.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2025-01-02", help="End date excluded")
    parser.add_argument("--out", default="raw-public")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--no-weather", action="store_true")
    args = parser.parse_args()
    print(json.dumps(ingest(args.start, args.end, args.out,
                            page_size=args.page_size, max_pages=args.max_pages,
                            with_weather=not args.no_weather), indent=2))


if __name__ == "__main__":
    main()
