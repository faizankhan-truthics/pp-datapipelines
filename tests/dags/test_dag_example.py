"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import logging
import os
from contextlib import contextmanager
from functools import lru_cache

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


@lru_cache(maxsize=None)
def get_dag_bag():
    """
    Parse the DAGs folder once per session. Every parametrize below (cases *and* their ids)
    reads this same DagBag -- building a fresh one per call let two parses disagree (e.g. a
    DAG hitting the import timeout on one parse only), which aborts collection with
    "In test_...: N parameter sets specified, with different number of ids".
    """
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def strip_path_prefix(path):
    return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))


def get_import_errors():
    """
    Generate a pytest.param per import error in the dag bag
    """
    # prepend a no-op case to ensure that a test object is always created.
    return [pytest.param(None, None, id="None")] + [
        pytest.param(strip_path_prefix(k), v.strip(), id=strip_path_prefix(k))
        for k, v in get_dag_bag().import_errors.items()
    ]


def get_dags():
    """
    Generate a pytest.param of dag_id, <DAG object>, fileloc per DAG in the DagBag
    """
    return [
        pytest.param(
            k, v, strip_path_prefix(v.fileloc), id=strip_path_prefix(v.fileloc)
        )
        for k, v in get_dag_bag().dags.items()
    ]


@pytest.mark.parametrize("rel_path,rv", get_import_errors())
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert dag.default_args.get("retries", None) >= 2, (
        f"{dag_id} in {fileloc} must have task retries >= 2."
    )
