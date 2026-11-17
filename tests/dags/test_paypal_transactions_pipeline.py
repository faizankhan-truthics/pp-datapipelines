from __future__ import annotations

from pathlib import Path

from airflow.models import DagBag

PAYPAL_DAG_DIR = Path(__file__).parents[2] / "dags" / "paypal"


def test_paypal_transactions_dag_wires_extract_load_merge_and_bronze_asset():
    dag_bag = DagBag(dag_folder=str(PAYPAL_DAG_DIR), include_examples=False)

    assert not dag_bag.import_errors, dag_bag.import_errors
    dag = dag_bag.dags["paypal_transactions_pipeline"]
    assert set(dag.task_ids) == {
        "extract_transactions",
        "collect_release_paths",
        "load_staging",
        "merge_bronze",
        "bronze_ready",
    }
    assert dag.get_task("extract_transactions").downstream_task_ids == {
        "collect_release_paths"
    }
    assert dag.get_task("bronze_ready").outlets[0].uri == "paypal://transactions/bronze"
