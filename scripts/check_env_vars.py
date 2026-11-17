#!/usr/bin/env python3
"""Check that every env var the code requires is defined where it will run.

Scans the Astro-deployed code (dags/, include/, plugins/) for variables it cannot run
without:

  * Variable.get("x")        -> AIRFLOW_VAR_X (Astro env vars need the AIRFLOW_VAR_ prefix to
                                be exposed as Airflow Variables, and Airflow uppercases the key)
  * os.environ["X"]          -> X (reads only; plain writes like os.environ["X"] = v are not
                                requirements, augmented assignments like += are)
  * {{ env_var('X') }} (dbt) -> X

Reads that carry a default (Variable.get("X", default_var=...), env_var('X', 'default')) are
optional and ignored. Then verifies each required name against one or more dotenv files
passed with --env-file, in the form `LABEL=path`:

    python scripts/check_env_vars.py --env-file ci=.env.ci
    python scripts/check_env_vars.py --env-file dev=dev.env --env-file prod=prod.env

Every label is checked against every read, including the ones inside task bodies: whether a
read happens at DAG parse time depends on how the code is called, which can't be told reliably
from the source.

Only key names are read from the env files, values are never printed. Astro deployments are
exported with `astro env variable export --deployment-id <id> --output <file>` (secret values
are left out unless --include-secrets is passed).

Names the runtime provides, or that are set outside Astro env vars (Airflow UI Variables),
go in scripts/env_vars_allowlist.txt, one name per line.
"""

import argparse
import ast
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PYTHON_DIRS = ("dags", "include", "plugins")
DBT_DIR = ROOT / "dags" / "dbt" / "bigquery_dbt"
DBT_SUFFIXES = {".sql", ".yml", ".yaml", ".csv"}
ALLOWLIST_PATH = ROOT / "scripts" / "env_vars_allowlist.txt"
AIRFLOW_VAR_PREFIX = "AIRFLOW_VAR_"

# Static names only: env_var('X') or env_var('X', default). env_var('X_' ~ suffix) is dynamic.
ENV_VAR_RE = re.compile(r"env_var\(\s*['\"]([^'\"]+)['\"]\s*([,)])")


def _rel(path: pathlib.Path) -> pathlib.Path:
    try:
        return path.relative_to(ROOT)
    except ValueError:
        return path


def _str_arg(
    node: ast.Call, index: int = 0, constants: dict[str, str] | None = None
) -> str | None:
    if len(node.args) > index:
        arg = node.args[index]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        # Variable.get(SOME_CONSTANT): resolve a module-level string constant
        if isinstance(arg, ast.Name) and constants:
            return constants.get(arg.id)
    return None


def _module_constants(tree: ast.AST) -> dict[str, str]:
    """Module-level `NAME = "literal"` assignments in one parsed file."""
    found: dict[str, str] = {}
    for node in getattr(tree, "body", []):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            found[node.targets[0].id] = node.value.value
    return found


def collect_constants() -> dict[str, str]:
    """String constants across the scanned code, so a read through a constant defined
    in another module (Variable.get(CAPI_API_KEY_VARIABLE)) is still seen. A name
    defined with different values in different files is ambiguous and left out."""
    values: dict[str, set[str]] = {}
    for top in PYTHON_DIRS:
        for path in sorted((ROOT / top).rglob("*.py")):
            if "dbt_packages" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(), filename=str(path))
            except SyntaxError:
                continue
            for name, value in _module_constants(tree).items():
                values.setdefault(name, set()).add(value)
    return {name: next(iter(vals)) for name, vals in values.items() if len(vals) == 1}


def _is_environ_subscript(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "environ"
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    )


def python_requirements(
    path: pathlib.Path, constants: dict[str, str] | None = None
) -> dict[str, str]:
    """Return {env var name: "file:line"} for the required reads in one Python file.
    `constants` are string constants from other modules (see collect_constants)."""
    found: dict[str, str] = {}
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError:
        return found
    rel = _rel(path)
    constants = {**(constants or {}), **_module_constants(tree)}

    # `os.environ["X"] += ...` reads the old value even though its context is Store
    augmented = {
        id(node.target)
        for node in ast.walk(tree)
        if isinstance(node, ast.AugAssign) and _is_environ_subscript(node.target)
    }

    for node in ast.walk(tree):
        name = None
        # Variable.get("x") with no default
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "Variable"
        ):
            var = _str_arg(node, constants=constants)
            has_default = len(node.args) > 1 or any(
                kw.arg in ("default_var", "default") for kw in node.keywords
            )
            if var and not has_default:
                name = AIRFLOW_VAR_PREFIX + var.upper()
        # os.environ["X"] read
        elif _is_environ_subscript(node) and (
            isinstance(node.ctx, ast.Load) or id(node) in augmented
        ):
            name = node.slice.value
        if name:
            found.setdefault(name, f"{rel}:{node.lineno}")
    return found


def dbt_requirements(path: pathlib.Path) -> dict[str, str]:
    """Return {env var name: "file:line"} for env_var('X') calls with no default."""
    found: dict[str, str] = {}
    rel = _rel(path)
    for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
        for match in ENV_VAR_RE.finditer(line):
            if match.group(2) == ")":
                found.setdefault(match.group(1), f"{rel}:{lineno}")
    return found


def collect_requirements() -> dict[str, str]:
    required: dict[str, str] = {}
    constants = collect_constants()
    for top in PYTHON_DIRS:
        for path in sorted((ROOT / top).rglob("*.py")):
            if "dbt_packages" in path.parts:
                continue
            for name, where in python_requirements(path, constants).items():
                required.setdefault(name, where)
    for path in sorted(DBT_DIR.rglob("*")):
        if (
            path.suffix in DBT_SUFFIXES
            and path.is_file()
            and not {"dbt_packages", "target", "logs"} & set(path.parts)
        ):
            for name, where in dbt_requirements(path).items():
                required.setdefault(name, where)
    return required


def read_allowlist() -> set[str]:
    if not ALLOWLIST_PATH.exists():
        return set()
    return {
        line.strip()
        for line in ALLOWLIST_PATH.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def read_env_keys(path: pathlib.Path) -> set[str]:
    keys = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            keys.add(line.split("=", 1)[0].strip().removeprefix("export "))
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="dotenv file to check the required names against (repeatable)",
    )
    args = parser.parse_args()

    required = collect_requirements()
    allowlist = read_allowlist()
    print(f"Found {len(required)} required env vars in code.")

    failed = False
    for spec in args.env_file:
        label, _, raw_path = spec.partition("=")
        path = pathlib.Path(raw_path)
        if not raw_path or not path.exists():
            print(f"[{label}] env file not found: {raw_path!r}")
            failed = True
            continue
        present = read_env_keys(path)
        missing = sorted(
            name for name in required if name not in present and name not in allowlist
        )
        if missing:
            failed = True
            print(
                f"\n[{label}] {len(missing)} required var(s) missing from {path.name}:"
            )
            for name in missing:
                print(f"  {name}    (read at {required[name]})")
        else:
            print(f"[{label}] OK, all required vars are set.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
