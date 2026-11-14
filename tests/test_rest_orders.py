import io
import json
import pytest
from integrations.rest_orders import fetch_orders, ingest_rest_orders
from src.lakehouse import run

class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self
    def __exit__(self, *_):
        self.close()

def opener(request, timeout):
    assert timeout == 15
    return FakeResponse(json.dumps([
        {"order_id":"R1","customer_id":"C1","order_date":"2026-11-14","amount":"19.5","status":"paid"}
    ]).encode())

def test_rest_ingestion_to_gold(tmp_path):
    target = tmp_path / "rest_orders.csv"
    assert ingest_rest_orders("https://example.org/api/orders", target, opener=opener) == 1
    result = run(target, tmp_path / "out")
    assert result["valid_unique_rows"] == 1
    assert result["quarantined_rows"] == 0

def test_rest_rejects_plain_http():
    with pytest.raises(ValueError): fetch_orders("http://example.org/orders", opener=opener)
