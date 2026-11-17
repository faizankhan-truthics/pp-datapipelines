FROM astrocrpublic.azurecr.io/runtime:3.3-2

# Putting the project root AND include/ on the path so shared code resolves the same way it does
# It is purely additive
# in Astro's prod DAG bundle (where the bundle root is on sys.path):
#   - /usr/local/airflow          -> `import include.config...` (matches prod; used by DAGs)
#   - /usr/local/airflow/include  -> bare `import config...` (back-compat)
# (Airflow already adds the dags/ folder to sys.path.)
ENV PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include

ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120
ENV AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=150

ENV AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False

ENV DBT_PACKAGES_INSTALL_PATH=/usr/local/airflow/.dbt_packages_cache

USER root

# The base image ships Python 3.14, which dbt and Elementary don't support yet
# (Elementary requires <3.14, dbt-bigquery targets <=3.13). Build the dbt virtualenv on
# Python 3.12 with uv (preinstalled in the runtime; it auto-downloads the 3.12 interpreter).
#
# UV_PYTHON_INSTALL_DIR pins the managed interpreter to a shared, world-readable path.
# Without it, uv installs Python under /root, and Airflow (which runs as the `astro` user)
# cannot traverse /root -> exec of dbt fails with "Permission denied". The final chmod makes
# the interpreter + venv readable/executable by `astro`.
ENV UV_PYTHON_INSTALL_DIR=/usr/local/share/uv/python
RUN uv python install 3.12 \
 && uv venv --python 3.12 /usr/local/airflow/dbt_venv \
 && uv pip install --python /usr/local/airflow/dbt_venv/bin/python \
      dbt-bigquery==1.11.3 \
      "elementary-data[bigquery]>=0.25.0,<0.26.0" \
 && chmod -R a+rX /usr/local/share/uv/python /usr/local/airflow/dbt_venv \
 # `edr` runs dbt against Elementary's bundled internal project and writes dbt_packages/
 # target/ inside it. That dir ships read-only in site-packages, so the `astro` runtime user
 # gets "Permission denied: 'dbt_packages'". Make just that project dir writable.
 && chmod -R a+rwX /usr/local/airflow/dbt_venv/lib/python3.12/site-packages/elementary/monitor/dbt_project

# airflow_local_settings.py only takes effect on `astro dev` (local); Astro Deployments
# never load it, so task-queue routing silently fell back to the `default` queue in the
# cloud. Installing this package registers the same routing as an `airflow.policy`
# setuptools entry point, which Airflow discovers identically in both environments.
# Copied explicitly (rather than relying on Astro's own include/ sync, which happens
# after this Dockerfile's RUN steps) so pip install below has the files to build from.
COPY include/pyproject.toml /usr/local/airflow/include/pyproject.toml
COPY include/queue_policy /usr/local/airflow/include/queue_policy
RUN pip install --no-cache-dir --no-deps /usr/local/airflow/include

# Pre-install dbt packages (dbt_utils, elementary) at build time, into the path configured
# by dbt_project.yml's `packages-install-path` (DBT_PACKAGES_INSTALL_PATH above) --
# deliberately outside dags/, so an Astro DAG-only deploy (which syncs/replaces dags/
# without rebuilding the image) never touches it. dbt_packages is gitignored, so without
# this it wouldn't exist at all on a DAG-only deploy, forcing Cosmos to fetch packages over
# the network on every DAG parse (dbt_dag.py's ProjectConfig.install_dbt_deps=False disables
# that, relying on this being pre-populated here). Only `dbt_project.yml` + `packages.yml`
# are needed for `dbt deps` -- no models, no profile, no BigQuery access -- so copying just
# those two keeps this layer cached across ordinary model changes and only invalidates when
# a dbt package version actually changes.
COPY dags/dbt/bigquery_dbt/dbt_project.yml dags/dbt/bigquery_dbt/packages.yml /tmp/dbt_deps_build/
RUN /usr/local/airflow/dbt_venv/bin/dbt deps --project-dir /tmp/dbt_deps_build \
 && chmod -R a+rwX "$DBT_PACKAGES_INSTALL_PATH" \
 && rm -rf /tmp/dbt_deps_build

USER astro