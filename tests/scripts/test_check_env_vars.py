import importlib.util
import pathlib
import textwrap

SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "check_env_vars.py"
spec = importlib.util.spec_from_file_location("check_env_vars", SCRIPT)
check_env_vars = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_env_vars)


def _python_file(tmp_path, source):
    path = tmp_path / "fixture.py"
    path.write_text(textwrap.dedent(source))
    return path


def test_python_reads_required_and_optional(tmp_path):
    path = _python_file(
        tmp_path,
        """
        import os
        from airflow.models import Variable

        A = Variable.get("A")
        B = Variable.get("B", default_var="x")
        C = os.environ["C"]
        D = os.environ.get("D")
        """,
    )
    found = check_env_vars.python_requirements(path)
    assert set(found) == {"AIRFLOW_VAR_A", "C"}


def test_reads_inside_functions_and_dag_factories_are_required(tmp_path):
    path = _python_file(
        tmp_path,
        """
        import os
        from airflow.models import Variable

        TOP = os.environ["TOP"]

        def task():
            return os.environ["INSIDE"]

        def dag_factory():
            return Variable.get("FACTORY")

        def with_default(x=os.environ["DEFAULT_ARG"]):
            pass
        """,
    )
    found = check_env_vars.python_requirements(path)
    assert set(found) == {"TOP", "INSIDE", "AIRFLOW_VAR_FACTORY", "DEFAULT_ARG"}


def test_variable_key_is_uppercased(tmp_path):
    path = _python_file(
        tmp_path,
        """
        from airflow.models import Variable

        A = Variable.get("my_key")
        """,
    )
    assert set(check_env_vars.python_requirements(path)) == {"AIRFLOW_VAR_MY_KEY"}


def test_environ_writes_are_not_requirements_but_augmented_assignments_are(tmp_path):
    path = _python_file(
        tmp_path,
        """
        import os

        os.environ["GENERATED"] = "value"
        os.environ["APPENDED"] += ":extra"
        del os.environ["REMOVED"]
        x = os.environ["READ"]
        """,
    )
    assert set(check_env_vars.python_requirements(path)) == {"APPENDED", "READ"}


def test_variable_read_through_a_constant_is_required(tmp_path):
    path = _python_file(
        tmp_path,
        """
        from airflow.models import Variable

        LOCAL_KEY = "local_key"

        a = Variable.get(LOCAL_KEY)
        b = Variable.get(IMPORTED_KEY)
        c = Variable.get(IMPORTED_OPTIONAL, default_var="x")
        d = Variable.get(UNKNOWN_NAME)
        e = Variable.get(IMPORTED_KEY, default=None)
        """,
    )
    found = check_env_vars.python_requirements(
        path, {"IMPORTED_KEY": "imported_key", "IMPORTED_OPTIONAL": "opt"}
    )
    assert set(found) == {"AIRFLOW_VAR_LOCAL_KEY", "AIRFLOW_VAR_IMPORTED_KEY"}


def test_constants_defined_twice_with_different_values_are_ambiguous(
    tmp_path, monkeypatch
):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "a.py").write_text('KEY = "one"\nSAME = "x"\n')
    (tmp_path / "dags" / "b.py").write_text('KEY = "two"\nSAME = "x"\n')
    monkeypatch.setattr(check_env_vars, "ROOT", tmp_path)
    monkeypatch.setattr(check_env_vars, "PYTHON_DIRS", ("dags",))
    assert check_env_vars.collect_constants() == {"SAME": "x"}


def test_dbt_env_var_skips_defaults_and_dynamic_names(tmp_path):
    path = tmp_path / "fixture.sql"
    path.write_text(
        "{{ env_var('NEEDED') }} {{ env_var('OPT', 'x') }} {{ env_var('DYN_' ~ x) }}"
    )
    found = check_env_vars.dbt_requirements(path)
    assert set(found) == {"NEEDED"}


def test_read_env_keys_ignores_values_and_comments(tmp_path):
    env = tmp_path / "x.env"
    env.write_text("# c\nA=1\nexport B=2\n\nbad line\n")
    assert check_env_vars.read_env_keys(env) == {"A", "B"}


def test_repo_requirements_are_in_env_ci():
    required = check_env_vars.collect_requirements()
    present = check_env_vars.read_env_keys(check_env_vars.ROOT / ".env.ci")
    allow = check_env_vars.read_allowlist()
    assert not [n for n in required if n not in present and n not in allow]
