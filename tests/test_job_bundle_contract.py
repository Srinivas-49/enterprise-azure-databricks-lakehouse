"""Preflight checks for paused, credential-free Databricks deployment definitions.

Not a live bundle validate or Databricks job execution.
"""
import ast
from pathlib import Path


def test_bootstrap_and_validation_notebooks_compile():
    for path in ("deployment/00_bootstrap_catalog.py", "deployment/90_validate_workspace.py"):
        ast.parse(Path(path).read_text(encoding="utf-8"))


def test_bundle_is_paused_and_has_ordered_quality_gate():
    content = Path("databricks.yml").read_text(encoding="utf-8")
    assert "pause_status: PAUSED" in content
    assert "notebook_path: ./deployment/00_bootstrap_catalog.py" in content
    assert "notebook_path: ./deployment/90_validate_workspace.py" in content
    assert content.index("task_key: bootstrap") < content.index("task_key: bronze_silver_cdc_gold")
    assert content.index("task_key: bronze_silver_cdc_gold") < content.index("task_key: multisource_gold")
    assert content.index("task_key: multisource_gold") < content.index("task_key: validate_all")
    assert "existing_cluster_id:" not in content
    assert "new_cluster:" not in content


def test_governance_sql_never_contains_secrets():
    for path in Path("governance").glob("*.sql"):
        text = path.read_text().lower()
        assert "sig=" not in text and "password=" not in text
