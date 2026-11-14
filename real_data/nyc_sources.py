"""Bounded, resumable NYC public-data ingestion and borough-month reconciliation.

Official Socrata REST datasets:
  311 service requests since 2020: erm2-nwe9
  NYC restaurant inspection violations: 43nn-pn8j
These are genuinely large public sources, but this program NEVER downloads the
entire source automatically. Run locally before uploading to Databricks Volumes.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ENDPOINT = "https://data.cityofnewyork.us/resource/"
SOURCES = {
    "service_requests": {
        "id": "erm2-nwe9",
        "select": "unique_key,created_date,closed_date,agency,complaint_type,borough,status",
        "order": "unique_key ASC",
        "date_field": "created_date",
    },
    "restaurant_inspections": {
        "id": "43nn-pn8j",
        "select": "camis,inspection_date,boro,critical_flag,grade,violation_code",
        "order": "camis ASC,inspection_date ASC,violation_code ASC",
        "date_field": "inspection_date",
    },
}
BOROUGHS = {"MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"}
BOROUGH_ALIASES = {
    "1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN",
    "4": "QUEENS", "5": "STATEN ISLAND"
}

def borough_name(raw):
    name = (str(raw or "")).strip().upper()
    return BOROUGH_ALIASES.get(name, name) if BOROUGH_ALIASES.get(name, name) in BOROUGHS else None

def month(value):
    """Use date substring only after strict ISO year/month validation."""
    text = str(value or "")
    try:
        datetime.strptime(text[:10], "%Y-%m-%d")
        return text[:7]
    except ValueError:
        return None

def http_json(url, *, retries=3, timeout=25, sleep=time.sleep):
    """Small, sequential HTTPS requests; fail visibly on 429/5xx after retry."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "PortfolioDE/1.0 (public research; see project README)",
                                        "Accept": "application/json"})
            with urlopen(req, timeout=timeout) as response:
                if response.geturl().split("/")[2] != "data.cityofnewyork.us":
                    raise ValueError("Unexpected data source redirect")
                return json.loads(response.read(8_000_000).decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            if attempt + 1 == retries or isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            sleep(min(2 ** attempt, 8))
    raise RuntimeError("unreachable")

def fetch_source(kind, output, *, page_size=1000, pages=2, after="2025-01-01", fetch=http_json, pause=0):
    """Write verified NDJSON page files with a resumable manifest.

    Offset pagination over a mutable dataset is NOT a stable snapshot.
    Pages are ordered and stored for reproducibility; after a live update,
    full or incremental refresh/reconciliation may be required.
    """
    if kind not in SOURCES:
        raise ValueError("Unknown dataset")
    if not 1 <= page_size <= 5000 or not 1 <= pages <= 1000:
        raise ValueError("page_size 1..5000; pages 1..1000")
    datetime.strptime(after, "%Y-%m-%d")
    cfg = SOURCES[kind]
    root = Path(output) / kind
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    if previous and (previous["source"] != kind or previous["from_date"] != after
                     or previous["page_size"] != page_size):
        raise ValueError("Cached download configuration differs; use a new landing folder")
    previous_entries = {x["name"]: x for x in (previous or {}).get("files", [])}
    manifest = {"source": kind, "dataset_id": cfg["id"], "from_date": after,
                "page_size": page_size, "files": []}
    for index in range(pages):
        params = {"$select": cfg["select"],
                  "$where": f"{cfg['date_field']} >= '{after}T00:00:00'",
                  "$order": cfg["order"], "$limit": page_size, "$offset": index * page_size}
        target = root / f"page_{index:06d}.jsonl"
        if target.exists():
            content = target.read_bytes()
            old_entry = previous_entries.get(target.name)
            if old_entry and hashlib.sha256(content).hexdigest() != old_entry["sha256"]:
                raise ValueError("Cached source page checksum mismatch: " + target.name)
            if not old_entry:
                raise ValueError("Untracked cached page; remove it or use a fresh directory")
            rows = len(content.splitlines())
        else:
            url = ENDPOINT + cfg["id"] + ".json?" + urlencode(params)
            payload = fetch(url)
            if not isinstance(payload, list):
                raise ValueError("Socrata returned non-list JSON")
            content = b"".join((json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
                               for record in payload)
            target.write_bytes(content)
            rows = len(payload)
            if pause:
                time.sleep(pause)
        manifest["files"].append({"name": target.name, "rows": rows,
                                  "sha256": hashlib.sha256(content).hexdigest()})
        if rows < page_size:
            break
    manifest["records_landed"] = sum(x["rows"] for x in manifest["files"])
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest

def read_landed(root, kind):
    folder = Path(root) / kind
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    for entry in manifest["files"]:
        raw = (folder / entry["name"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError("Source file checksum mismatch: " + entry["name"])
        for line in raw.splitlines():
            yield json.loads(line)

def aggregate(root):
    """Monthly operational metrics; no residential address fields are ingested."""
    requests = defaultdict(int)
    agency_requests = defaultdict(int)
    seen_requests = set()
    invalid = {"service_requests": 0, "restaurant_inspections": 0}
    for item in read_landed(root, "service_requests"):
        b, m = borough_name(item.get("borough")), month(item.get("created_date"))
        key = str(item.get("unique_key") or "")
        if not b or not m or not key:
            invalid["service_requests"] += 1
            continue
        if key in seen_requests:
            continue
        seen_requests.add(key)
        requests[(b, m)] += 1
        agency_requests[(b, m, item.get("agency") or "UNSPECIFIED")] += 1
    inspections = defaultdict(set)
    critical = defaultdict(int)
    for item in read_landed(root, "restaurant_inspections"):
        b, m = borough_name(item.get("boro")), month(item.get("inspection_date"))
        camis = str(item.get("camis") or "")
        if not b or not m or not camis or m == "1900-01":
            invalid["restaurant_inspections"] += 1
            continue
        inspection_key = (camis, str(item.get("inspection_date")))
        inspections[(b, m)].add(inspection_key)
        if item.get("critical_flag") == "Critical":
            critical[(b, m)] += 1
    rows = []
    for b, m in sorted(set(requests) | set(inspections)):
        rows.append({"borough": b, "month": m, "service_requests": requests[(b, m)],
                     "distinct_restaurant_inspections": len(inspections[(b, m)]),
                     "critical_violation_citations": critical[(b, m)]})
    return {"rows": rows, "invalid_rows": invalid,
            "unique_311_requests": len(seen_requests),
            "request_agency_groups": len(agency_requests)}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", default="real-data/nyc")
    p.add_argument("--pages", type=int, default=2)
    p.add_argument("--page-size", type=int, default=1000)
    p.add_argument("--after", default="2025-01-01")
    p.add_argument("--confirm-large", action="store_true")
    args = p.parse_args()
    if args.pages > 10 and not args.confirm_large:
        p.error("More than 10 pages requires --confirm-large (API load and local disk use)")
    for kind in SOURCES:
        print(json.dumps(fetch_source(kind, args.output, pages=args.pages,
                                      page_size=args.page_size, after=args.after), indent=2))
    result = aggregate(args.output)
    (Path(args.output) / "borough_month_metrics.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"groups": len(result["rows"]),
                      "unique_311_requests": result["unique_311_requests"],
                      "invalid": result["invalid_rows"]}, indent=2))

if __name__ == "__main__":
    main()
