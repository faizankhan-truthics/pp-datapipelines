"""Unit tests for include/config/posthog_settings.py.

* get_posthog_projects(): the seed CSV's posthog_id column is the only list of PostHog
  projects, so a bad value must fail loudly instead of checking the wrong project (or the
  same one twice).
* ENV: the Development/Production split decides whether the data checks alert at all.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import include.config.posthog_settings as posthog_settings

HEADER = "ga4_property_name,product_name,ga4_property_id,posthog_id\n"


def _seed(tmp_path: Path, body: str, header: str = HEADER) -> Path:
    path = tmp_path / "seed.csv"
    path.write_text(header + body, encoding="utf-8")
    return path


class TestGetPosthogProjects:
    def test_real_seed_has_files_editor_and_docuhero(self):
        assert posthog_settings.get_posthog_projects() == [
            {"project_id": 315092, "product_name": "files-editor.com"},
            {"project_id": 674694, "product_name": "docuhero.com"},
        ]

    def test_empty_values_are_skipped(self, tmp_path):
        seed = _seed(tmp_path, "a,a.com,1,\nb,b.com,2,42\nc,c.com,3,  \n")

        assert posthog_settings.get_posthog_projects(seed) == [
            {"project_id": 42, "product_name": "b.com"}
        ]

    def test_duplicate_id_fails(self, tmp_path):
        seed = _seed(tmp_path, "a,a.com,1,42\nb,b.com,2,42\n")

        with pytest.raises(ValueError, match="42 is set on both 'a.com' and 'b.com'"):
            posthog_settings.get_posthog_projects(seed)

    @pytest.mark.parametrize("bad", ["abc", "42.0", "-1", "4 2"])
    def test_non_integer_id_fails(self, tmp_path, bad):
        seed = _seed(tmp_path, f'a,a.com,1,"{bad}"\n')

        with pytest.raises(ValueError, match="not an integer"):
            posthog_settings.get_posthog_projects(seed)

    def test_missing_column_fails(self, tmp_path):
        seed = _seed(
            tmp_path,
            "a,a.com,1\n",
            header="ga4_property_name,product_name,ga4_property_id\n",
        )

        with pytest.raises(ValueError, match="no posthog_id column"):
            posthog_settings.get_posthog_projects(seed)

    def test_product_name_falls_back_to_property_name(self, tmp_path):
        seed = _seed(tmp_path, "uat,,1,7\n")

        assert posthog_settings.get_posthog_projects(seed) == [
            {"project_id": 7, "product_name": "uat"}
        ]


@pytest.fixture
def reload_with_env(monkeypatch):
    def _reload(**env):
        for key, value in env.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)
        return importlib.reload(posthog_settings)

    yield _reload
    monkeypatch.undo()
    importlib.reload(posthog_settings)


class TestEnv:
    def test_gold_sandbox_is_development(self, reload_with_env):
        assert (
            reload_with_env(AIRFLOW_VAR_BQ_GOLD_DATASET="gold_sandbox").ENV
            == "Development"
        )

    @pytest.mark.parametrize("gold", ["gold", None])
    def test_anything_else_is_production(self, reload_with_env, gold):
        assert reload_with_env(AIRFLOW_VAR_BQ_GOLD_DATASET=gold).ENV == "Production"
