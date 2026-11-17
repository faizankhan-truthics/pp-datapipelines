"""Unit tests for the GrowthBook GCS operator (include/custom_operators/growthbook/growthbook_to_gcs_operator.py).

* ``land_and_release_to_gcs`` lands every raw page losslessly and always uploads a
  Release Avro -- also with zero records, because an endpoint can legitimately be empty
  and the staging load must still find a file.
* ``GrowthBookToGCSOperator.execute`` builds the right hook for the endpoint and picks
  the ``ds`` folder date (data_interval start, or the instant of a manual run).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from airflow.sdk.exceptions import AirflowFailException
from fastavro import reader as avro_reader

import include.custom_operators.growthbook.growthbook_to_gcs_operator as gb_operator
from include.config.growthbook_settings import ENDPOINT_PARAMS, ENDPOINTS
from include.custom_operators.growthbook.growthbook_to_gcs_operator import (
    GrowthBookToGCSOperator,
    land_and_release_to_gcs,
)


class FakeBlob:
    def __init__(self):
        self.content_type = None
        self.chunk_size = None
        self.written_lines: list[str] = []
        self.uploaded_records = None

    def open(self, mode):
        blob = self

        class _Ctx:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def write(self, data):
                blob.written_lines.append(data)

        return _Ctx()

    def upload_from_filename(self, path):
        # the temp file is deleted right after upload, so read it now
        with open(path, "rb") as fh:
            self.uploaded_records = list(avro_reader(fh))


class FakeBucket:
    def __init__(self):
        self._blobs: dict[str, FakeBlob] = {}

    def blob(self, path):
        return self._blobs.setdefault(path, FakeBlob())


class FakeHook:
    def __init__(self, pages, records_key="features"):
        self._pages = pages
        self.records_key = records_key
        self.received_max_pages = "UNSET"

    def iter_raw_pages(self, *, max_pages=None):
        self.received_max_pages = max_pages
        yield from self._pages

    def extract_records(self, payload):
        return payload.get(self.records_key) or []


def _patch_gcs(monkeypatch, bucket: FakeBucket):
    fake_client = SimpleNamespace(bucket=lambda name: bucket)
    monkeypatch.setattr(
        gb_operator,
        "GCSHook",
        lambda **kwargs: SimpleNamespace(get_conn=lambda: fake_client),
    )


def _land(hook, entity="features"):
    return land_and_release_to_gcs(
        hook=hook,
        entity=entity,
        ds="2026-10-14",
        run_id="scheduled__2026-10-14T06:00:00+00:00",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )


class TestLandAndRelease:
    def test_zero_records_still_uploads_an_empty_release_avro(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        pages = [{"features": [], "hasMore": False, "nextOffset": None}]

        result = _land(FakeHook(pages))

        assert result["record_count"] == 0
        assert result["page_count"] == 1
        assert bucket.blob(result["release_path"]).uploaded_records == []

    def test_lands_every_page_and_releases_every_record(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        pages = [
            {"features": [{"id": "flag_1"}], "hasMore": True, "nextOffset": 100},
            {
                "features": [{"id": "flag_2"}, {"id": "flag_3"}],
                "hasMore": False,
                "nextOffset": None,
            },
        ]

        result = _land(FakeHook(pages))

        assert result["record_count"] == 3
        landed = [
            json.loads(line)
            for line in "".join(
                bucket.blob(result["landing_path"]).written_lines
            ).splitlines()
        ]
        assert landed == pages
        released = bucket.blob(result["release_path"]).uploaded_records
        assert [json.loads(r["raw_data"])["id"] for r in released] == [
            "flag_1",
            "flag_2",
            "flag_3",
        ]
        assert {r["_elt_endpoint"] for r in released} == {"features"}
        assert {r["_gcs_file_path"] for r in released} == {
            f"gs://test-bucket/{result['release_path']}"
        }

    def test_paths_follow_the_landing_release_layout(self, monkeypatch):
        _patch_gcs(monkeypatch, FakeBucket())

        result = _land(FakeHook([{"features": [], "hasMore": False}]))

        assert result["landing_path"].startswith(
            "landing/growthbook/features/2026-10-14/"
        )
        assert result["release_path"].startswith(
            "release/growthbook/features/2026-10-14/"
        )
        assert result["release_path"].endswith(".avro")
        assert ":" not in result["release_path"] and "+" not in result["release_path"]

    def test_skips_non_dict_records(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)

        result = _land(
            FakeHook([{"features": [{"id": "flag_1"}, "junk", None], "hasMore": False}])
        )

        assert result["record_count"] == 1

    def test_passes_the_page_cap_to_the_hook(self, monkeypatch):
        _patch_gcs(monkeypatch, FakeBucket())
        hook = FakeHook([{"features": [], "hasMore": False}])

        _land(hook)

        assert hook.received_max_pages == gb_operator.MAX_PAGES_PER_RUN


class TestOperatorExecute:
    def _execute(self, monkeypatch, endpoint, start, end):
        captured = {}

        def fake_land(**kwargs):
            captured.update(kwargs)
            return {
                "page_count": 1,
                "record_count": 1,
                "landing_path": "",
                "release_path": "",
            }

        monkeypatch.setattr(gb_operator, "land_and_release_to_gcs", fake_land)
        op = GrowthBookToGCSOperator(
            task_id=f"extract_{endpoint}", api_key="secret_test", endpoint=endpoint
        )
        op.execute(
            {"data_interval_start": start, "data_interval_end": end, "run_id": "run"}
        )
        return captured

    @pytest.mark.parametrize("endpoint", list(ENDPOINTS))
    def test_builds_the_hook_for_the_endpoint(self, monkeypatch, endpoint):
        start = datetime(2026, 10, 14, tzinfo=timezone.utc)
        captured = self._execute(
            monkeypatch, endpoint, start, datetime(2026, 10, 15, tzinfo=timezone.utc)
        )

        path, records_key = ENDPOINTS[endpoint]
        assert captured["hook"].path == path
        assert captured["hook"].records_key == records_key
        assert captured["hook"].params == ENDPOINT_PARAMS.get(endpoint, {})
        assert captured["entity"] == endpoint

    def test_an_empty_snapshot_fails_the_task(self, monkeypatch):
        # Landing zero flags would be green end to end and leave bronze silently stale.
        monkeypatch.setattr(
            gb_operator,
            "land_and_release_to_gcs",
            lambda **kwargs: {
                "page_count": 1,
                "record_count": 0,
                "landing_path": "",
                "release_path": "",
            },
        )
        start = datetime(2026, 10, 14, tzinfo=timezone.utc)
        op = GrowthBookToGCSOperator(
            task_id="extract_features", api_key="secret_test", endpoint="features"
        )

        with pytest.raises(AirflowFailException, match="no records"):
            op.execute(
                {
                    "data_interval_start": start,
                    "data_interval_end": start,
                    "run_id": "run",
                }
            )

    def test_scheduled_run_uses_the_data_interval_start(self, monkeypatch):
        captured = self._execute(
            monkeypatch,
            "features",
            datetime(2026, 10, 14, tzinfo=timezone.utc),
            datetime(2026, 10, 15, tzinfo=timezone.utc),
        )
        assert captured["ds"] == "2026-10-14"

    def test_manual_run_with_collapsed_interval_uses_that_instant(self, monkeypatch):
        instant = datetime(2026, 10, 15, 13, 30, tzinfo=timezone.utc)
        captured = self._execute(monkeypatch, "features", instant, instant)
        assert captured["ds"] == "2026-10-15"

    def _execute_without_interval(self, monkeypatch, context):
        captured = {}

        def fake_land(**kwargs):
            captured.update(kwargs)
            return {
                "page_count": 1,
                "record_count": 1,
                "landing_path": "",
                "release_path": "",
            }

        monkeypatch.setattr(gb_operator, "land_and_release_to_gcs", fake_land)
        GrowthBookToGCSOperator(
            task_id="extract_features", api_key="secret_test", endpoint="features"
        ).execute(context)
        return captured

    def test_manual_run_without_a_data_interval_uses_the_run_after_date(
        self, monkeypatch
    ):
        # Airflow 3: a run triggered by hand with no logical date has no data_interval_* keys at all
        run_after = datetime(2026, 10, 20, 17, 14, tzinfo=timezone.utc)
        context = {
            "run_id": "manual__x",
            "dag_run": SimpleNamespace(run_after=run_after),
        }
        assert (
            self._execute_without_interval(monkeypatch, context)["ds"] == "2026-10-20"
        )

    def test_none_data_interval_is_treated_like_a_missing_one(self, monkeypatch):
        run_after = datetime(2026, 10, 20, 23, 30, tzinfo=timezone.utc)
        context = {
            "run_id": "manual__x",
            "data_interval_start": None,
            "data_interval_end": None,
            "dag_run": SimpleNamespace(run_after=run_after),
        }
        assert (
            self._execute_without_interval(monkeypatch, context)["ds"] == "2026-10-20"
        )

    def test_run_after_in_another_timezone_is_converted_to_utc(self, monkeypatch):
        from datetime import timedelta

        run_after = datetime(
            2026, 10, 21, 1, 30, tzinfo=timezone(timedelta(hours=3))
        )  # 22:30 UTC on the 20th
        context = {
            "run_id": "manual__x",
            "dag_run": SimpleNamespace(run_after=run_after),
        }
        assert (
            self._execute_without_interval(monkeypatch, context)["ds"] == "2026-10-20"
        )
