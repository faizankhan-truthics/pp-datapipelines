"""Unit tests for LandingToReleaseOperator (include/custom_operators/exchange_rates/landing_to_release.py).

Despite living under the exchange_rates package, this operator is the generic,
source-agnostic step 2 of the landing/release/BigQuery pattern (its own docstring
says so: "Used by both the treasury_fx and google_finance_fx pipelines"). These
tests cover the pieces most likely to silently corrupt a release or wedge a DAG:

  * ``_read_landing_pages`` -- refuses to read a landing drop with no `_SUCCESS`
    marker, and only ever aggregates ``*.json`` pages (never `_SUCCESS` itself).
  * ``_dedup`` -- last-page-wins per natural key, deterministic sort order.
  * ``_checksum`` -- must be insensitive to incidental dict key ordering (relies
    on ``json.dumps(..., sort_keys=True)``) so a no-op re-run is correctly
    recognized as "unchanged" rather than spuriously re-releasing every day.
  * ``execute`` -- the checksum-gate itself: identical content skips (raises
    AirflowSkipException) rather than re-writing a release file; changed content
    writes a new release + manifest and pushes the row-count XCom.

No real GCS call is made anywhere: GCSHook is monkeypatched to an in-memory fake
(mirroring tests/include/custom_operators/solidgate/test_solidgate_to_gcs_operator.py).
"""

from __future__ import annotations

import json

import pytest
from airflow.exceptions import AirflowSkipException

import include.custom_operators.exchange_rates.landing_to_release as mod
from include.custom_operators.exchange_rates.landing_to_release import (
    LandingToReleaseOperator,
)


class FakeGCSHook:
    """In-memory object store keyed by object_name; good enough to stand in for
    the handful of GCSHook methods this operator calls (exists/list/download/upload).
    """

    def __init__(self, objects: dict | None = None):
        self.objects: dict[str, bytes] = {}
        for name, content in (objects or {}).items():
            self.objects[name] = (
                content.encode("utf-8") if isinstance(content, str) else content
            )
        self.uploads: list[dict] = []

    def exists(self, bucket_name, object_name):
        return object_name in self.objects

    def list(self, bucket_name, prefix=""):
        return [name for name in self.objects if name.startswith(prefix)]

    def download(self, bucket_name, object_name):
        return self.objects[object_name]

    def upload(self, bucket_name, object_name, data, mime_type=None):
        payload = data.encode("utf-8") if isinstance(data, str) else data
        self.objects[object_name] = payload
        self.uploads.append(
            {"object_name": object_name, "data": data, "mime_type": mime_type}
        )


def _landing_objects(
    prefix: str, pages: list[dict], extra_non_json: dict | None = None
) -> dict:
    objects = {f"{prefix}/_SUCCESS": json.dumps({"pages": len(pages)})}
    for i, page in enumerate(pages, start=1):
        objects[f"{prefix}/page_{i:04d}.json"] = json.dumps(page)
    if extra_non_json:
        objects.update(extra_non_json)
    return objects


class FakeTaskInstance:
    def __init__(self, xcom_values: dict | None = None):
        self._xcom_values = xcom_values or {}
        self.pushed: dict = {}

    def xcom_pull(self, task_ids):
        return self._xcom_values.get(task_ids)

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _make_operator(**overrides):
    kwargs = dict(
        task_id="landing_to_release",
        gcs_bucket="test-bucket",
        landing_task_id="extract_to_landing",
        release_prefix="release/fx_bronze/daily_snapshot",
        manifest_object="release/fx_bronze/daily_snapshot/_manifest/latest_checksum.txt",
        natural_key_fields=["currency", "date"],
    )
    kwargs.update(overrides)
    return LandingToReleaseOperator(**kwargs)


def _patch_gcs(monkeypatch, hook: FakeGCSHook):
    monkeypatch.setattr(mod, "GCSHook", lambda **kwargs: hook)


class TestReadLandingPages:
    def test_raises_when_no_success_marker_present(self):
        op = _make_operator()
        hook = FakeGCSHook({"landing/run1/page_0001.json": json.dumps({"data": []})})

        with pytest.raises(RuntimeError, match="No _SUCCESS marker"):
            op._read_landing_pages(hook, "landing/run1")

    def test_aggregates_data_across_multiple_json_pages_and_skips_non_json(self):
        op = _make_operator()
        pages = [
            {"data": [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]},
            {"data": [{"currency": "PLN", "date": "2026-08-18", "close": 4.1}]},
        ]
        objects = _landing_objects(
            "landing/run1",
            pages,
            extra_non_json={"landing/run1/readme.txt": "not json"},
        )
        hook = FakeGCSHook(objects)

        rows = op._read_landing_pages(hook, "landing/run1")

        assert sorted(rows, key=lambda r: r["currency"]) == [
            {"currency": "EUR", "date": "2026-08-18", "close": 1.08},
            {"currency": "PLN", "date": "2026-08-18", "close": 4.1},
        ]

    def test_success_marker_itself_is_not_treated_as_a_data_page(self):
        op = _make_operator()
        # _SUCCESS does not end in .json, so it must never be parsed as a data page.
        objects = _landing_objects(
            "landing/run1", [{"data": [{"currency": "EUR", "date": "2026-08-18"}]}]
        )
        hook = FakeGCSHook(objects)

        rows = op._read_landing_pages(hook, "landing/run1")

        assert rows == [{"currency": "EUR", "date": "2026-08-18"}]


class TestDedup:
    def test_later_row_wins_on_duplicate_natural_key(self):
        op = _make_operator()
        rows = [
            {"currency": "EUR", "date": "2026-08-18", "close": 1.00},
            {
                "currency": "EUR",
                "date": "2026-08-18",
                "close": 1.08,
            },  # same key, later in the list
        ]

        deduped = op._dedup(rows)

        assert deduped == [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]

    def test_sorted_by_natural_key_fields_as_strings(self):
        op = _make_operator()
        rows = [
            {"currency": "PLN", "date": "2026-08-18", "close": 4.1},
            {"currency": "EUR", "date": "2026-08-18", "close": 1.08},
        ]

        deduped = op._dedup(rows)

        assert [r["currency"] for r in deduped] == ["EUR", "PLN"]

    def test_missing_natural_key_field_raises_key_error(self):
        op = _make_operator()
        rows = [{"currency": "EUR"}]  # missing "date"

        with pytest.raises(KeyError):
            op._dedup(rows)


class TestChecksum:
    def test_identical_rows_with_differently_ordered_dict_keys_hash_the_same(self):
        op = _make_operator()
        rows_a = [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]
        rows_b = [{"close": 1.08, "date": "2026-08-18", "currency": "EUR"}]

        assert op._checksum(rows_a) == op._checksum(rows_b)

    def test_different_content_hashes_differently(self):
        op = _make_operator()
        rows_a = [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]
        rows_b = [{"currency": "EUR", "date": "2026-08-18", "close": 1.09}]

        assert op._checksum(rows_a) != op._checksum(rows_b)

    def test_row_order_matters_to_the_checksum(self):
        """_checksum itself does not sort -- callers (execute, via _dedup) are
        responsible for handing it a deterministically ordered list. This test
        documents that _checksum is order-sensitive, so it only produces a stable
        result when fed already-sorted input.
        """
        op = _make_operator()
        rows_a = [
            {"currency": "EUR", "date": "2026-08-18"},
            {"currency": "PLN", "date": "2026-08-18"},
        ]
        rows_b = [
            {"currency": "PLN", "date": "2026-08-18"},
            {"currency": "EUR", "date": "2026-08-18"},
        ]

        assert op._checksum(rows_a) != op._checksum(rows_b)


class TestExecute:
    def _context(
        self,
        hook: FakeGCSHook,
        *,
        landing_run_prefix="landing/run1",
        run_id="scheduled__2026-08-18T00:00:00+00:00",
    ):
        ti = FakeTaskInstance({"extract_to_landing": landing_run_prefix})
        return {"ti": ti, "run_id": run_id}, ti

    def test_raises_when_no_landing_run_prefix_xcom(self, monkeypatch):
        op = _make_operator()
        hook = FakeGCSHook()
        _patch_gcs(monkeypatch, hook)
        ti = FakeTaskInstance({})  # no xcom pushed by the extract task
        context = {"ti": ti, "run_id": "scheduled__2026-08-18T00:00:00+00:00"}

        with pytest.raises(RuntimeError, match="No landing_run_prefix XCom"):
            op.execute(context)

    def test_raises_when_landing_has_a_success_marker_but_zero_rows(self, monkeypatch):
        op = _make_operator()
        hook = FakeGCSHook(_landing_objects("landing/run1", [{"data": []}]))
        _patch_gcs(monkeypatch, hook)
        context, _ti = self._context(hook)

        with pytest.raises(RuntimeError, match="zero rows"):
            op.execute(context)

    def test_writes_release_and_manifest_when_content_is_new(self, monkeypatch):
        op = _make_operator()
        rows = [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]
        hook = FakeGCSHook(_landing_objects("landing/run1", [{"data": rows}]))
        _patch_gcs(monkeypatch, hook)
        # No ":" or "+" here on purpose -- the sanitization itself is covered by
        # test_run_id_with_colons_and_plus_is_sanitized_in_the_release_path below.
        context, ti = self._context(hook, run_id="scheduled__2026-08-18T00_00_00")

        release_path = op.execute(context)

        assert (
            release_path
            == "release/fx_bronze/daily_snapshot/scheduled__2026-08-18T00_00_00/data.jsonl"
        )
        assert json.loads(hook.objects[release_path].decode("utf-8")) == rows[0]
        assert hook.objects[op.manifest_object].decode("utf-8") == op._checksum(rows)
        assert ti.pushed["release_row_count"] == 1

    def test_run_id_with_colons_and_plus_is_sanitized_in_the_release_path(
        self, monkeypatch
    ):
        op = _make_operator()
        rows = [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]
        hook = FakeGCSHook(_landing_objects("landing/run1", [{"data": rows}]))
        _patch_gcs(monkeypatch, hook)
        context, _ti = self._context(
            hook, run_id="scheduled__2026-08-18T00:00:00+00:00"
        )

        release_path = op.execute(context)

        assert ":" not in release_path
        assert "+" not in release_path
        assert (
            release_path
            == "release/fx_bronze/daily_snapshot/scheduled__2026-08-18T00_00_00_00_00/data.jsonl"
        )

    def test_skips_via_airflow_skip_exception_when_checksum_unchanged(
        self, monkeypatch
    ):
        op = _make_operator()
        rows = [{"currency": "EUR", "date": "2026-08-18", "close": 1.08}]
        objects = _landing_objects("landing/run1", [{"data": rows}])
        objects[op.manifest_object] = op._checksum(
            rows
        )  # pre-seed with the SAME checksum
        hook = FakeGCSHook(objects)
        _patch_gcs(monkeypatch, hook)
        context, _ti = self._context(hook)
        uploads_before = list(hook.uploads)

        with pytest.raises(AirflowSkipException):
            op.execute(context)

        # Nothing new was written -- the whole point of the checksum gate.
        assert hook.uploads == uploads_before

    def test_custom_clean_row_fn_is_applied_before_dedup_and_checksum(
        self, monkeypatch
    ):
        def clean(raw):
            return {**raw, "close": float(raw["close"])}

        op = _make_operator(clean_row_fn=clean)
        rows = [
            {"currency": "EUR", "date": "2026-08-18", "close": "1.08"}
        ]  # close arrives as a string
        hook = FakeGCSHook(_landing_objects("landing/run1", [{"data": rows}]))
        _patch_gcs(monkeypatch, hook)
        context, _ti = self._context(hook)

        release_path = op.execute(context)

        released_row = json.loads(hook.objects[release_path].decode("utf-8"))
        assert released_row["close"] == 1.08
        assert isinstance(released_row["close"], float)

    def test_default_clean_row_fn_passes_rows_through_unchanged(self, monkeypatch):
        op = _make_operator()  # no clean_row_fn override -> _identity_clean_row
        rows = [
            {"currency": "EUR", "date": "2026-08-18", "close": "1.08"}
        ]  # left as a string
        hook = FakeGCSHook(_landing_objects("landing/run1", [{"data": rows}]))
        _patch_gcs(monkeypatch, hook)
        context, _ti = self._context(hook)

        release_path = op.execute(context)

        released_row = json.loads(hook.objects[release_path].decode("utf-8"))
        assert released_row["close"] == "1.08"  # untouched -- identity means identity
