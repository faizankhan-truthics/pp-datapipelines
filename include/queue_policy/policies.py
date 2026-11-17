"""
Route TaskInstances to Astro worker queues based on run type.

- Native Airflow Backfill runs -> ``backfill`` queue.
- ``run_type=backfill`` (via dag_run.conf) -> ``backfill`` queue.
- Otherwise -> ``daily`` queue.

Registered as an Airflow cluster policy through the ``airflow.policy``
setuptools entry point (see pyproject.toml) instead of
include/airflow_local_settings.py. Astro Deployments never load
airflow_local_settings.py -- only local `astro dev` does -- so tasks were
silently falling back to the ``default`` queue in the cloud. An installed
entry-point plugin is discovered by Airflow the same way locally and on
Astro, so this hook now applies everywhere. See the is_noop flip below for
why the entry point alone isn't enough.
"""

from __future__ import annotations

from airflow import settings as airflow_settings
from airflow.policies import hookimpl
from airflow.utils.types import DagRunType

RUN_TYPE_PARAM = "run_type"
DEFAULT_QUEUE = "daily"
VALID_QUEUES = {"daily", "backfill"}

# DagRun.verify_integrity bulk-inserts newly created TaskInstances (skipping
# task_instance_mutation_hook entirely) unless this flag is False. Airflow
# only clears it from import_local_settings(), i.e. only when an importable
# airflow_local_settings module happens to exist -- registering a hook purely
# via an `airflow.policy` entry point never flips it on its own. Flip it here
# so the hook above actually runs for freshly created task instances.
airflow_settings.task_instance_mutation_hook.is_noop = False


def resolve_queue(dag_run) -> str:
    if dag_run is None:
        return DEFAULT_QUEUE

    if dag_run.run_type == DagRunType.BACKFILL_JOB:
        return "backfill"

    queue = (dag_run.conf or {}).get(RUN_TYPE_PARAM, DEFAULT_QUEUE)
    return queue if queue in VALID_QUEUES else DEFAULT_QUEUE


@hookimpl
def task_instance_mutation_hook(task_instance, dag_run) -> None:
    task_instance.queue = resolve_queue(dag_run)
