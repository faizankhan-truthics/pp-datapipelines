"""Unit tests for the Astro worker-queue routing policy
(include/queue_policy/policies.py).

This is the hook that decides whether a task instance runs on the ``daily``
or ``backfill`` worker queue. It's the prime suspect for why
TriggerDagRunOperator-triggered and manually-triggered "backfill" runs have
been unreliable: anything other than a native Airflow CLI backfill
(``DagRunType.BACKFILL_JOB``) or an explicit ``conf["run_type"] == "backfill"``
silently falls through to the ``daily`` queue (sized for daily volume, not
backfill concurrency) with no error at all. These tests pin exactly which
inputs route where, so that gap is visible rather than implicit.
"""

from __future__ import annotations

from types import SimpleNamespace

from airflow.utils.types import DagRunType

from include.queue_policy.policies import (
    DEFAULT_QUEUE,
    resolve_queue,
    task_instance_mutation_hook,
)


def _dag_run(run_type=DagRunType.SCHEDULED, conf=None):
    return SimpleNamespace(run_type=run_type, conf=conf)


class TestResolveQueue:
    def test_no_dag_run_at_all_falls_back_to_default_queue(self):
        assert resolve_queue(None) == DEFAULT_QUEUE

    def test_native_cli_backfill_always_routes_to_backfill_queue(self):
        dag_run = _dag_run(run_type=DagRunType.BACKFILL_JOB, conf=None)

        assert resolve_queue(dag_run) == "backfill"

    def test_native_cli_backfill_wins_even_if_conf_says_otherwise(self):
        """run_type=BACKFILL_JOB is checked first and unconditionally -- a
        conflicting conf value should never override it."""
        dag_run = _dag_run(run_type=DagRunType.BACKFILL_JOB, conf={"run_type": "daily"})

        assert resolve_queue(dag_run) == "backfill"

    def test_explicit_conf_run_type_backfill_routes_to_backfill_queue(self):
        dag_run = _dag_run(run_type=DagRunType.MANUAL, conf={"run_type": "backfill"})

        assert resolve_queue(dag_run) == "backfill"

    def test_explicit_conf_run_type_daily_routes_to_daily_queue(self):
        dag_run = _dag_run(run_type=DagRunType.MANUAL, conf={"run_type": "daily"})

        assert resolve_queue(dag_run) == "daily"

    def test_scheduled_run_with_no_conf_at_all_falls_back_to_default_queue(self):
        dag_run = _dag_run(run_type=DagRunType.SCHEDULED, conf=None)

        assert resolve_queue(dag_run) == DEFAULT_QUEUE

    def test_manually_triggered_run_with_no_run_type_conf_key_falls_back_to_daily(self):
        """The known gap: a manual trigger or TriggerDagRunOperator call that
        forgets to set conf['run_type'] silently lands on 'daily', not
        'backfill', with no error or warning anywhere."""
        dag_run = _dag_run(run_type=DagRunType.MANUAL, conf={})

        assert resolve_queue(dag_run) == DEFAULT_QUEUE

    def test_unrecognized_run_type_value_in_conf_falls_back_to_default_queue(self):
        dag_run = _dag_run(
            run_type=DagRunType.MANUAL, conf={"run_type": "not_a_real_queue"}
        )

        assert resolve_queue(dag_run) == DEFAULT_QUEUE

    def test_other_conf_keys_are_ignored(self):
        dag_run = _dag_run(
            run_type=DagRunType.MANUAL,
            conf={"accounts": ["acme"], "endpoints": ["charges"]},
        )

        assert resolve_queue(dag_run) == DEFAULT_QUEUE


class TestTaskInstanceMutationHook:
    def test_sets_task_instance_queue_to_the_resolved_queue(self):
        task_instance = SimpleNamespace(queue=None)
        dag_run = _dag_run(run_type=DagRunType.BACKFILL_JOB, conf=None)

        task_instance_mutation_hook(task_instance, dag_run)

        assert task_instance.queue == "backfill"

    def test_sets_task_instance_queue_to_default_when_nothing_indicates_backfill(self):
        task_instance = SimpleNamespace(queue=None)
        dag_run = _dag_run(run_type=DagRunType.MANUAL, conf=None)

        task_instance_mutation_hook(task_instance, dag_run)

        assert task_instance.queue == DEFAULT_QUEUE
