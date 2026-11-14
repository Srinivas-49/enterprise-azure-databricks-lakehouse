"""REST API order ingestion adapter with dependency-free HTTP and deterministic CSV landing.

Uses HTTPS only. No production endpoint or authorization tokens are committed.
"""
import json
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from integrations.kafka_orders import append_order_batch

def fetch_orders(url, token=None, timeout=15, opener=urlopen):
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("REST endpoint must be HTTPS")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, headers=headers, method="GET")
    with opener(request, timeout=timeout) as response:
        payload = json.load(response)
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError("expected a list of order objects")
    return payload

def ingest_rest_orders(url, output_csv, token=None, opener=urlopen):
    orders = fetch_orders(url, token=token, opener=opener)
    append_order_batch(orders, output_csv)
    return len(orders)
