"""Unit tests for StripeToGCSOperator.execute (include/custom_operators/stripe/stripe_to_gcs_operator.py).

The operator streams each Stripe API page straight to GCS (one landing JSON file and one
release Avro file per page) instead of buffering a whole day's pages/records in memory
first -- this bounds peak memory to ~1 page per thread regardless of day size, which is
what was OOMing backfills on high-volume endpoints. Before writing a day's pages, the
operator also deletes any objects already sitting under that account+day's directory, so
a rerun that produces fewer pages than a previous run doesn't leave stale page files
behind for the BigQuery load (which loads the exact paths returned by this run) to
double-count.

Both the landing and release directories are keyed by account_name and the actual data day
(``ds_str``), never by the run's trigger date -- each account+day gets its own directory, so
a same-instant manual trigger's folder now matches the day it actually fetched (it used to
be one day ahead, since the folder was pinned to the trigger date). Filenames follow
``{account_name}_{endpoint}_{page_index}.json``/``.avro`` (e.g. ``acme_charges_00001.avro``)
-- no run-type suffix, since the directory already disambiguates account/day and a single
page index is unique within it. Because the release folder can now differ per page, the DAG
that loads these files into BigQuery (dags/stripe/stripe_financial_pipeline.py) collects the
exact ``release_paths`` this operator returns via XCom instead of globbing a single wildcard
folder.

StripeHook and GCSHook are monkeypatched below -- no network/GCS is used. The fake GCS
double models just enough of the real google-cloud-storage API (``get_conn().bucket(...)
.list_blobs()/.delete_blobs()`` and ``bucket.client.batch()``) for the cleanup step to be
exercised.
"""

from __future__ import annotations

import io
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import fastavro

import include.custom_operators.stripe.stripe_to_gcs_operator as sto_operator
from include.custom_operators.stripe.stripe_to_gcs_operator import StripeToGCSOperator


class FakeStripeHook:
    """Stands in for the StripeHook CLASS: patched in as a callable that returns itself,
    so ``StripeHook(api_key_env=..., account_id=...)`` (called once per day, per thread, inside
    the operator) always yields the same configured pages regardless of account/day."""

    def __init__(self, pages_per_day):
        self._pages_per_day = pages_per_day
        self.get_raw_pages_calls = []
        self.construction_kwargs = []

    def __call__(self, api_key_env, account_id):
        self.construction_kwargs.append(
            {"api_key_env": api_key_env, "account_id": account_id}
        )
        return self

    def get_raw_pages(self, endpoint, start_ts, end_ts):
        self.get_raw_pages_calls.append((endpoint, start_ts, end_ts))
        yield from self._pages_per_day


def _patch_stripe_hook(monkeypatch, pages_per_day):
    fake = FakeStripeHook(pages_per_day)
    monkeypatch.setattr(sto_operator, "StripeHook", fake)
    return fake


class FakeBucket:
    """Minimal stand-in for google.cloud.storage.Bucket: tracks live object names so
    list_blobs/delete_blobs can be exercised by the operator's cleanup step."""

    def __init__(self, existing_objects=()):
        self.objects = set(existing_objects)
        self.deleted = []
        self.delete_batches = []  # sizes of each delete_blobs call made inside client.batch()
        self.client = FakeClient(self)

    def list_blobs(self, prefix):
        return [
            SimpleNamespace(name=name)
            for name in self.objects
            if name.startswith(prefix)
        ]

    def delete_blobs(self, blobs):
        assert self.client.in_batch, "deletes must run inside client.batch()"
        self.delete_batches.append(len(blobs))
        for blob in blobs:
            self.objects.discard(blob.name)
            self.deleted.append(blob.name)


class FakeClient:
    """Stands in for storage.Client: only batch(), as a context manager. Counts open
    batches (not a bool) since per-day worker threads share this fake."""

    def __init__(self, bucket):
        self._open_batches = 0
        self._lock = threading.Lock()

    @property
    def in_batch(self):
        return self._open_batches > 0

    @contextmanager
    def batch(self, raise_exception=True):
        with self._lock:
            self._open_batches += 1
        try:
            yield
        finally:
            with self._lock:
                self._open_batches -= 1


def _patch_gcs(monkeypatch, existing_objects=()):
    uploads: list[dict] = []
    bucket = FakeBucket(existing_objects)

    def upload(**kw):
        uploads.append(kw)
        bucket.objects.add(kw["object_name"])

    fake_hook = SimpleNamespace(
        upload=upload, get_conn=lambda: SimpleNamespace(bucket=lambda name: bucket)
    )
    monkeypatch.setattr(sto_operator, "GCSHook", lambda **kwargs: fake_hook)
    return uploads, bucket


def _operator(**overrides) -> StripeToGCSOperator:
    return StripeToGCSOperator(
        task_id=overrides.get("task_id", "extract_test"),
        account_name=overrides.get("account_name", "acme"),
        account_id=overrides.get("account_id", "acct_123"),
        api_key_env=overrides.get("api_key_env", "STRIPE_API_KEY_TEST"),
        endpoint=overrides.get("endpoint", "charges"),
        gcs_bucket=overrides.get("gcs_bucket", "test-bucket"),
        max_workers=overrides.get("max_workers", 4),
    )


def _context(*, conf=None, start, end, params=None):
    return {
        "dag_run": SimpleNamespace(conf=conf or {}),
        "data_interval_start": start,
        "data_interval_end": end,
        "params": params or {},
    }


ONE_CHARGE_PAGE = [
    {
        "data": [{"id": "ch_1", "object": "charge", "livemode": False, "amount": 100}],
        "has_more": False,
    }
]


def _avro_records(uploads, object_name):
    upload = next(u for u in uploads if u["object_name"] == object_name)
    return list(fastavro.reader(io.BytesIO(upload["data"])))


class TestDateWindowMath:
    def test_single_day_scheduled_run_processes_exactly_the_data_interval(
        self, monkeypatch
    ):
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        start = datetime(2026, 5, 10, 1, 0, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, 1, 0, tzinfo=timezone.utc)
        ctx = _context(start=start, end=end)

        result = op.execute(ctx)

        assert (
            result["release_path"]
            == "release/stripe/charges/acme/2026-05-10/acme_charges_00001.avro"
        )
        assert stripe_hook.get_raw_pages_calls == [
            ("charges", int(start.timestamp()), int(end.timestamp()))
        ]

    def test_same_instant_manual_trigger_covers_the_previous_utc_day(self, monkeypatch):
        """A same-instant manual trigger processes the previous full UTC day, like a scheduled
        run -- a 14:30:05 trigger processes [yesterday 00:00, today 00:00), not the 24h
        before the trigger instant.
        """
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        now = datetime(2026, 8, 18, 14, 30, 5, tzinfo=timezone.utc)
        ctx = _context(start=now, end=now)

        result = op.execute(ctx)

        day_start = datetime(2026, 8, 17, tzinfo=timezone.utc)
        day_end = datetime(2026, 8, 18, tzinfo=timezone.utc)
        assert (
            result["release_path"]
            == "release/stripe/charges/acme/2026-08-17/acme_charges_00001.avro"
        )
        assert stripe_hook.get_raw_pages_calls == [
            ("charges", int(day_start.timestamp()), int(day_end.timestamp()))
        ]

    def test_trigger_without_logical_date_falls_back_to_run_after(self, monkeypatch):
        """Airflow 3 leaves data_interval_start/end out of the context for runs triggered
        without a logical date; the operator used to raise KeyError on them."""
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        now = datetime(2026, 8, 18, 14, 30, 5, tzinfo=timezone.utc)
        ctx = {"dag_run": SimpleNamespace(conf={}, run_after=now), "params": {}}

        result = op.execute(ctx)

        day_start = datetime(2026, 8, 17, tzinfo=timezone.utc)
        day_end = datetime(2026, 8, 18, tzinfo=timezone.utc)
        assert (
            result["release_path"]
            == "release/stripe/charges/acme/2026-08-17/acme_charges_00001.avro"
        )
        assert stripe_hook.get_raw_pages_calls == [
            ("charges", int(day_start.timestamp()), int(day_end.timestamp()))
        ]

    def test_multi_day_backfill_processes_one_window_per_day(self, monkeypatch):
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        uploads, _bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        # data_interval is irrelevant once conf carries an explicit backfill window -- each
        # day gets its own release folder keyed by that day, regardless of the trigger date.
        ctx = _context(
            conf={"date_from": "2026-01-01", "date_to": "2026-01-04"},
            start=datetime(2026, 6, 1, tzinfo=timezone.utc),
            end=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert (
            "release_path" not in result
        )  # only set when there's exactly one release path
        assert sorted(result["release_paths"]) == [
            "release/stripe/charges/acme/2026-01-01/acme_charges_00001.avro",
            "release/stripe/charges/acme/2026-01-02/acme_charges_00001.avro",
            "release/stripe/charges/acme/2026-01-03/acme_charges_00001.avro",
        ]
        assert len(stripe_hook.get_raw_pages_calls) == 3  # Jan 1, 2, 3
        landing_paths = sorted(
            u["object_name"] for u in uploads if "landing/" in u["object_name"]
        )
        assert landing_paths == [
            "landing/stripe/charges/acme/2026-01-01/acme_charges_00001.json",
            "landing/stripe/charges/acme/2026-01-02/acme_charges_00001.json",
            "landing/stripe/charges/acme/2026-01-03/acme_charges_00001.json",
        ]

    def test_two_same_day_backfills_with_different_windows_do_not_collide_on_release_path(
        self, monkeypatch
    ):
        """Each day's release folder is keyed by that day (conf['date_from']/['date_to']),
        never by the DAG run's trigger data_interval -- so two backfills manually triggered
        on the same calendar day for different historical windows land in entirely
        different folders.
        """
        _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        trigger_instant = datetime(2026, 6, 1, 9, 0, tzinfo=timezone.utc)

        op_january = _operator(endpoint="charges", account_name="acme")
        ctx_january = _context(
            conf={"date_from": "2026-01-01", "date_to": "2026-01-02"},
            start=trigger_instant,
            end=trigger_instant,
        )
        result_january = op_january.execute(ctx_january)

        op_february = _operator(endpoint="charges", account_name="acme")
        ctx_february = _context(
            conf={"date_from": "2026-02-01", "date_to": "2026-02-02"},
            start=trigger_instant,
            end=trigger_instant,
        )
        result_february = op_february.execute(ctx_february)

        assert result_january["release_path"] != result_february["release_path"]


class TestAllowlistFiltering:
    def test_endpoint_not_in_allowlist_returns_empty_result_instead_of_skipping(
        self, monkeypatch
    ):
        """Returning an empty result (rather than raising AirflowSkipException) keeps this
        mapped instance's XCom populated -- the DAG's downstream collect_release_paths task
        reads every mapped instance's .output, and a skipped instance pushes no XCom at all,
        which can crash that collection step."""
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            conf={"endpoints": ["invoices"]},
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }
        assert stripe_hook.get_raw_pages_calls == []

    def test_account_not_in_allowlist_returns_empty_result_instead_of_skipping(
        self, monkeypatch
    ):
        stripe_hook = _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            conf={"accounts": ["someone_else"]},
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }
        assert stripe_hook.get_raw_pages_calls == []


class TestRecordShapingAndAlwaysUploadingReleaseAvro:
    def test_balance_endpoint_treats_the_whole_page_as_a_single_record(
        self, monkeypatch
    ):
        pages = [{"available": [{"amount": 500, "currency": "usd"}], "pending": []}]
        _patch_stripe_hook(monkeypatch, pages)
        uploads, _bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="balance", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert result["landing_pages_processed"] == 1
        release_upload = next(
            u for u in uploads if u["object_name"] == result["release_path"]
        )
        assert release_upload["mime_type"] == "associated-avro-binary"

    def test_metadata_noise_keys_are_stripped_from_raw_data(self, monkeypatch):
        _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        uploads, _bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        raw_records = [
            json.loads(r["raw_data"])
            for r in _avro_records(uploads, result["release_path"])
        ]
        assert raw_records == [
            {"id": "ch_1", "amount": 100}
        ]  # object/livemode stripped

    def test_zero_records_still_writes_and_uploads_a_release_avro_unlike_solidgate(
        self, monkeypatch
    ):
        _patch_stripe_hook(monkeypatch, [{"data": [], "has_more": False}])
        uploads, _bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert result["landing_pages_processed"] == 1
        release_upload = next(
            u for u in uploads if u["object_name"] == result["release_path"]
        )
        assert (
            release_upload["mime_type"] == "associated-avro-binary"
        )  # uploaded even with 0 records

    def test_a_day_with_zero_pages_no_longer_crashes_and_yields_no_release_files(
        self, monkeypatch
    ):
        """Previously a real bug: a zero-page day never reached the Release Avro
        write/upload/return (both gated behind ``if day_pages:``), so ``fetch_day_data``
        implicitly returned None for that day and ``day_results.sort(...)`` blew up with a
        TypeError, crashing the whole task on a perfectly normal quiet day. Per-page writes
        remove that gate entirely: a day with 0 pages just produces 0 files instead of
        crashing.
        """
        _patch_stripe_hook(monkeypatch, [])  # generator yields nothing at all
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }

    def test_each_page_is_written_as_its_own_landing_and_release_file(
        self, monkeypatch
    ):
        pages = [
            {
                "data": [{"id": "ch_1", "object": "charge", "livemode": False}],
                "has_more": True,
            },
            {
                "data": [{"id": "ch_2", "object": "charge", "livemode": False}],
                "has_more": False,
            },
        ]
        _patch_stripe_hook(monkeypatch, pages)
        uploads, _bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        result = op.execute(ctx)

        assert sorted(result["release_paths"]) == [
            "release/stripe/charges/acme/2026-05-10/acme_charges_00001.avro",
            "release/stripe/charges/acme/2026-05-10/acme_charges_00002.avro",
        ]
        landing_paths = sorted(
            u["object_name"] for u in uploads if "landing/" in u["object_name"]
        )
        assert landing_paths == [
            "landing/stripe/charges/acme/2026-05-10/acme_charges_00001.json",
            "landing/stripe/charges/acme/2026-05-10/acme_charges_00002.json",
        ]
        first_page_records = [
            json.loads(r["raw_data"])
            for r in _avro_records(uploads, result["release_paths"][0])
        ]
        assert first_page_records == [{"id": "ch_1"}]


class TestStaleFileCleanup:
    def test_prior_run_leftover_page_files_are_deleted_before_this_run_writes(
        self, monkeypatch
    ):
        """Simulates a rerun where a previous execution wrote 3 pages for this account+day
        but this run only produces 1. Without cleanup, pages 2 and 3 would linger and get
        double-counted by the BigQuery load."""
        _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        stale_landing = "landing/stripe/charges/acme/2026-05-10/acme_charges_00002.json"
        stale_release = "release/stripe/charges/acme/2026-05-10/acme_charges_00002.avro"
        other_account_release = (
            "release/stripe/charges/other/2026-05-10/other_charges_00001.avro"
        )
        uploads, bucket = _patch_gcs(
            monkeypatch,
            existing_objects=[stale_landing, stale_release, other_account_release],
        )
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        op.execute(ctx)

        assert stale_landing in bucket.deleted
        assert stale_release in bucket.deleted
        # Cleanup must be scoped to this account's own directory -- it must never touch
        # another account's files, even though they share the same endpoint/day.
        assert other_account_release not in bucket.deleted
        assert other_account_release in bucket.objects

    def test_large_leftover_directory_is_deleted_in_batches_of_100(self, monkeypatch):
        """A day left over from a big unfiltered run can hold thousands of page files;
        one HTTP call per delete stalled the task for minutes before any Stripe call."""
        _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        stale = [
            f"landing/stripe/charges/acme/2026-05-10/acme_charges_{i:05d}.json"
            for i in range(2, 252)
        ]
        _uploads, bucket = _patch_gcs(monkeypatch, existing_objects=stale)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        op.execute(ctx)

        assert bucket.delete_batches == [100, 100, 50]
        assert set(stale) <= set(bucket.deleted)

    def test_cleanup_is_a_noop_when_nothing_stale_exists(self, monkeypatch):
        _patch_stripe_hook(monkeypatch, ONE_CHARGE_PAGE)
        uploads, bucket = _patch_gcs(monkeypatch)
        op = _operator(endpoint="charges", account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
        )

        op.execute(ctx)

        assert bucket.deleted == []
