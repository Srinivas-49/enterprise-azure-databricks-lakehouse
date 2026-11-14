"""Optional Apache Airflow 2.x DAG for scheduled local lakehouse transformations.

Requires apache-airflow installed on a scheduler/worker with project code.
This is a reference DAG, not evidence of a running managed Airflow instance.
"""
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

ROOT = Path(__file__).resolve().parents[1]

@dag(dag_id="orders_medallion_lakehouse", start_date=datetime(2026, 1, 1),
     schedule="@daily", catchup=False, max_active_runs=1,
     tags=["data-engineering", "lakehouse"])
def medallion_lakehouse():
    @task(retries=2)
    def build_gold():
        from src.lakehouse import run
        source = ROOT / "data" / "orders.csv"
        output = ROOT / "outputs" / "airflow"
        return run(str(source), str(output))
    build_gold()

dag = medallion_lakehouse()
