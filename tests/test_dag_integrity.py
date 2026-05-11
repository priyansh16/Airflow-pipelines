"""
DAG Integrity Tests
====================
These tests run in CI on every push to main.
They catch broken DAGs before they ever reach the Airflow scheduler.
Compatible with Airflow 3.x on Python 3.13.
"""
import os
import sys
import pytest

# Add dags folder to path so imports resolve correctly
DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")
INCLUDE_FOLDER = os.path.join(os.path.dirname(__file__), "..", "include")
sys.path.insert(0, os.path.abspath(INCLUDE_FOLDER))

from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    """
    Load all DAGs once for the entire test session.
    scope=session means this runs once, not once per test.
    """
    bag =  DagBag(
        dag_folder=os.path.abspath(DAG_FOLDER),
        include_examples=False
    )
    
    # include subdirectories
    for subfolder in ["etl", "elt"]:
        sub_path = os.path.join(os.path.abspath(DAG_FOLDER), subfolder)
        sub_bag = DagBag(
            dag_folder=sub_path,
            include_examples=False,
        )
        bag.dags.update(sub_bag.dags)
        bag.import_errors.update(sub_bag.import_errors)
    
    return bag


class TestDagIntegrity:

    def test_no_import_errors(self, dag_bag):
        """Every DAG file must import without errors."""
        assert dag_bag.import_errors == {}, (
            "DAG import errors found:\n"
            + "\n".join(
                f"  {dag}: {error}"
                for dag, error in dag_bag.import_errors.items()
            )
        )

    def test_required_dags_exist(self, dag_bag):
        """Both pipeline DAGs must be present."""
        required_dags = ["financial_news_etl", "ecommerce_elt"]
        for dag_id in required_dags:
            assert dag_id in dag_bag.dag_ids, (
                f"Required DAG '{dag_id}' not found. "
                f"Available DAGs: {list(dag_bag.dag_ids)}"
            )

    def test_dags_have_tags(self, dag_bag):
        """Every DAG must have at least one tag."""
        for dag_id, dag in dag_bag.dags.items():
            assert len(dag.tags) > 0, (
                f"DAG '{dag_id}' has no tags."
            )

    def test_dags_have_retries(self, dag_bag):
        """Every task must have at least 1 retry configured."""
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                assert task.retries >= 1, (
                    f"Task '{task.task_id}' in DAG '{dag_id}' "
                    f"has no retries."
                )

    def test_dags_have_descriptions(self, dag_bag):
        """Every DAG must have a doc_md description."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.doc_md is not None, (
                f"DAG '{dag_id}' has no doc_md description."
            )

    def test_etl_dag_task_count(self, dag_bag):
        """ETL DAG must have at least 5 tasks."""
        assert "financial_news_etl" in dag_bag.dags, (
            "financial_news_etl not found"
        )
        dag = dag_bag.dags["financial_news_etl"]
        assert len(dag.tasks) >= 5, (
            f"financial_news_etl has {len(dag.tasks)} tasks, "
            f"expected at least 5"
        )

    def test_elt_dag_task_count(self, dag_bag):
        """ELT DAG must have at least 4 tasks."""
        assert "ecommerce_elt" in dag_bag.dags, (
            "ecommerce_elt not found"
        )
        dag = dag_bag.dags["ecommerce_elt"]
        assert len(dag.tasks) >= 4, (
            f"ecommerce_elt has {len(dag.tasks)} tasks, "
            f"expected at least 4"
        )

    def test_catchup_is_disabled(self, dag_bag):
        """Catchup must be disabled on all DAGs."""
        for dag_id, dag in dag_bag.dags.items():
            assert not dag.catchup, (
                f"DAG '{dag_id}' has catchup=True. "
                f"Set catchup=False."
            )