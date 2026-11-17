"""Unit tests for extract_events_to_gcs (include/custom_operators/stripe/stripe_to_gcs_operator.py): Stripe
/v1/events fetched as one-hour windows (threaded within a day, days one after another)
into per-hour GCS folders. StripeHook and GCSHook are monkeypatched -- no network/GCS."""

from __future__ import annotations

import io
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import fastavro
import pytest

import include.custom_operators.stripe.stripe_to_gcs_operator as events_module
from include.custom_operators.stripe.stripe_to_gcs_operator import extract_events_to_gcs

ONE_EVENT_PAGE = [
    {
        "data": [
            {
                "id": "evt_1",
                "object": "event",
                "livemode": False,
                "type": "customer.subscription.updated",
            }
        ],
        "has_more": False,
    }
]


class FakeStripeHook:
    """Patched in for the StripeHook class: every construction returns this one fake, which
    records each (start_ts, end_ts) window it is asked for, in call order."""

    def __init__(self, pages):
        self._pages = pages
        self.calls = []
        self._lock = threading.Lock()

    def __call__(self, api_key_env, account_id):
        return self

    def get_raw_pages(self, endpoint, start_ts, end_ts):
        assert endpoint == "events"
        with self._lock:
            self.calls.append((start_ts, end_ts))
        yield from self._pages


class FakeBucket:
    def __init__(self, existing_objects=()):
        self.objects = set(existing_objects)
        self.deleted = []
        self._lock = threading.Lock()
        self.client = SimpleNamespace(batch=self._batch)

    @contextmanager
    def _batch(self):
        yield

    def list_blobs(self, prefix):
        with self._lock:
            return [
                SimpleNamespace(name=name)
                for name in self.objects
                if name.startswith(prefix)
            ]

    def delete_blobs(self, blobs):
        with self._lock:
            for blob in blobs:
                self.objects.discard(blob.name)
                self.deleted.append(blob.name)


@pytest.fixture
def gcs(monkeypatch):
    uploads = []
    bucket = FakeBucket()
    contents = {}

    def upload(**kw):
        uploads.append(kw)
        bucket.objects.add(kw["object_name"])
        contents[kw["object_name"]] = kw["data"]

    def exists(bucket_name, object_name):
        return object_name in bucket.objects

    def download(bucket_name, object_name):
        data = contents[object_name]
        return data.encode() if isinstance(data, str) else data

    fake_hook = SimpleNamespace(
        upload=upload,
        exists=exists,
        download=download,
        get_conn=lambda: SimpleNamespace(bucket=lambda name: bucket),
    )
    monkeypatch.setattr(events_module, "GCSHook", lambda **kwargs: fake_hook)
    return SimpleNamespace(uploads=uploads, bucket=bucket)


@pytest.fixture
def stripe(monkeypatch):
    fake = FakeStripeHook(ONE_EVENT_PAGE)
    monkeypatch.setattr(events_module, "StripeHook", fake)
    return fake


def _ts(*args):
    return int(datetime(*args, tzinfo=timezone.utc).timestamp())


def _run(
    *,
    conf=None,
    run_after=datetime(2026, 6, 1, 1, 10, tzinfo=timezone.utc),
    account_name="acme",
    run_id=None,
):
    context = {
        "dag_run": SimpleNamespace(conf=conf or {}, run_after=run_after, run_id=run_id)
    }
    return extract_events_to_gcs(
        context=context,
        account_name=account_name,
        account_id="acct_123",
        api_key_env="STRIPE_API_KEY_TEST",
        gcs_bucket="test-bucket",
        gcp_conn_id="gcp",
        max_workers=4,
    )


class TestWindows:
    def test_run_after_financial_pipeline_fetches_the_previous_utc_day_as_24_hours(
        self, stripe, gcs
    ):
        # stripe_financial_pipeline starts at 00:00 UTC and finishes ~01:10, triggering this run.
        result = _run(run_after=datetime(2026, 5, 11, 1, 10, tzinfo=timezone.utc))

        assert result["release_paths"] == [
            f"release/stripe/events/acme/2026-05-10/hour-{h:02d}/acme_events_00001.avro"
            for h in range(24)
        ]
        assert sorted(stripe.calls) == [
            (_ts(2026, 5, 10, h), _ts(2026, 5, 10, h) + 3600) for h in range(24)
        ]
        assert any(
            u["object_name"]
            == "landing/stripe/events/acme/2026-05-10/hour-14/acme_events_00001.json"
            for u in gcs.uploads
        )

    def test_backfill_runs_days_one_after_another(self, stripe, gcs):
        result = _run(
            conf={"date_from": "2026-05-10", "date_to": "2026-05-13"},
        )

        assert len(result["release_paths"]) == 72
        # Every hour of a day is fetched before any hour of the next day.
        call_days = [
            datetime.fromtimestamp(start, timezone.utc).date()
            for start, _ in stripe.calls
        ]
        assert call_days == sorted(call_days)

    def test_backfill_with_iso_datetimes_fetches_only_those_hours(self, stripe, gcs):
        result = _run(
            conf={"date_from": "2026-01-01T22:00:00", "date_to": "2026-01-02T01:00:00"},
        )

        assert result["release_paths"] == [
            "release/stripe/events/acme/2026-01-01/hour-22/acme_events_00001.avro",
            "release/stripe/events/acme/2026-01-01/hour-23/acme_events_00001.avro",
            "release/stripe/events/acme/2026-01-02/hour-00/acme_events_00001.avro",
        ]

    def test_mid_day_manual_trigger_still_fetches_the_previous_full_utc_day(
        self, stripe, gcs
    ):
        result = _run(run_after=datetime(2026, 8, 18, 15, 42, tzinfo=timezone.utc))

        assert len(result["release_paths"]) == 24
        assert min(stripe.calls)[0] == _ts(2026, 8, 17, 0)
        assert max(stripe.calls)[1] == _ts(2026, 8, 18, 0)

    def test_backfill_end_before_start_is_rejected(self, stripe, gcs):
        with pytest.raises(ValueError, match="date_to"):
            _run(
                conf={"date_from": "2026-05-10", "date_to": "2026-05-10"},
            )


class TestOutput:
    def test_account_not_in_allowlist_returns_empty_result(self, stripe, gcs):
        result = _run(
            conf={"accounts": ["other"]},
        )

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }
        assert stripe.calls == []

    def test_release_avro_keeps_every_top_level_event_key(self, stripe, gcs):
        result = _run(
            conf={"date_from": "2026-05-10T14:00:00", "date_to": "2026-05-10T15:00:00"},
        )

        upload = next(
            u for u in gcs.uploads if u["object_name"] == result["release_path"]
        )
        [record] = list(fastavro.reader(io.BytesIO(upload["data"])))
        assert record["_elt_endpoint"] == "events"
        assert record["_elt_stripe_account_id"] == "acct_123"
        assert json.loads(record["raw_data"]) == ONE_EVENT_PAGE[0]["data"][0]

    def test_rerun_clears_stale_files_in_that_hour_folder_only(self, stripe, gcs):
        stale = "release/stripe/events/acme/2026-05-10/hour-14/acme_events_00002.avro"
        other_hour = (
            "release/stripe/events/acme/2026-05-10/hour-15/acme_events_00001.avro"
        )
        gcs.bucket.objects.update({stale, other_hour})

        _run(
            conf={"date_from": "2026-05-10T14:00:00", "date_to": "2026-05-10T15:00:00"},
        )

        assert stale in gcs.bucket.deleted
        assert other_hour not in gcs.bucket.deleted


class TestRetry:
    THREE_HOURS = {"date_from": "2026-05-10T14:00:00", "date_to": "2026-05-10T17:00:00"}

    def test_retry_in_same_run_refetches_only_hours_that_failed(self, monkeypatch, gcs):
        failing_hour = _ts(2026, 5, 10, 15)

        class FlakyStripeHook(FakeStripeHook):
            fail = True

            def get_raw_pages(self, endpoint, start_ts, end_ts):
                with self._lock:
                    self.calls.append((start_ts, end_ts))
                if start_ts == failing_hour and FlakyStripeHook.fail:
                    raise ConnectionError("read timed out")
                yield from self._pages

        flaky = FlakyStripeHook(ONE_EVENT_PAGE)
        monkeypatch.setattr(events_module, "StripeHook", flaky)

        with pytest.raises(ConnectionError):
            _run(conf=self.THREE_HOURS, run_id="run_1")
        assert len(flaky.calls) == 3

        FlakyStripeHook.fail = False
        flaky.calls.clear()
        result = _run(conf=self.THREE_HOURS, run_id="run_1")

        assert flaky.calls == [(failing_hour, failing_hour + 3600)]
        assert result["release_paths"] == [
            f"release/stripe/events/acme/2026-05-10/hour-{h}/acme_events_00001.avro"
            for h in (14, 15, 16)
        ]
        assert result["total_records"] == 3

    def test_new_run_refetches_hours_finished_by_an_earlier_run(self, stripe, gcs):
        _run(conf=self.THREE_HOURS, run_id="run_1")
        stripe.calls.clear()

        _run(conf=self.THREE_HOURS, run_id="run_2")

        assert len(stripe.calls) == 3

    def test_marker_is_not_a_release_avro(self, stripe, gcs):
        result = _run(conf=self.THREE_HOURS, run_id="run_1")

        assert (
            "release/stripe/events/acme/2026-05-10/hour-14/_SUCCESS.json"
            in gcs.bucket.objects
        )
        assert all(path.endswith(".avro") for path in result["release_paths"])
