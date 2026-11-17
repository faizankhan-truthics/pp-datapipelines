"""Unit tests for ChargebackhitToGCSOperator.execute
(include/custom_operators/chargebackhit/chargebackhit_to_gcs.py).

Refactored to follow Stripe's structure (include/custom_operators/stripe/stripe_to_gcs_operator.py,
see tests/include/custom_operators/stripe/test_stripe_to_gcs_operator.py): each API page is
streamed straight to GCS as its own landing JSON file and its own release Avro file (one Avro
row per record, not one row per page), instead of buffering the whole run's records in memory
and writing a single combined release file at the end. This bounds peak memory to ~1 page per
thread regardless of backfill size, and -- because each page's release path is keyed by the
account name and the *actual* data day (``ds_str``), not the run's trigger date and a fixed
filename suffix -- it also fixes a real path-collision bug the old implementation had: two
same-day backfills for different historical windows used to resolve to the identical release
path and silently overwrite each other.

Before writing a day's pages, the operator also deletes any objects already sitting under that
account+day's directory, so a rerun that produces fewer pages than a previous run doesn't leave
stale page files behind for the BigQuery load (which loads the exact paths this run returns) to
double-count.

ChargebackhitHook and GCSHook are monkeypatched below -- no network/GCS is used. The fake GCS
double models just enough of the real google-cloud-storage API (``get_conn().bucket(...)
.list_blobs()/.delete_blobs()``) for the cleanup step to be exercised.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import fastavro

import include.custom_operators.chargebackhit.chargebackhit_to_gcs as cb_operator
from include.custom_operators.chargebackhit.chargebackhit_to_gcs import (
    ChargebackhitToGCSOperator,
)


class FakeChargebackhitHook:
    """Stands in for the ChargebackhitHook CLASS: patched in as a callable that records
    constructor kwargs and returns pages keyed by (start_ts, end_ts)."""

    def __init__(self, pages_by_window):
        self._pages_by_window = pages_by_window
        self.get_raw_pages_calls = []
        self.construction_kwargs = []

    def __call__(self, *, account_name, public_key_env, secret_key_env):
        self.construction_kwargs.append(
            {
                "account_name": account_name,
                "public_key_env": public_key_env,
                "secret_key_env": secret_key_env,
            }
        )
        return self

    def get_raw_pages(self, endpoint, start_ts, end_ts):
        self.get_raw_pages_calls.append((endpoint, start_ts, end_ts))
        yield from self._pages_by_window.get((start_ts, end_ts), [])


def _patch_chargebackhit_hook(monkeypatch, pages_by_window):
    fake = FakeChargebackhitHook(pages_by_window)
    monkeypatch.setattr(cb_operator, "ChargebackhitHook", fake)
    return fake


class FakeBucket:
    """Minimal stand-in for google.cloud.storage.Bucket: tracks live object names so
    list_blobs/delete_blobs can be exercised by the operator's cleanup step."""

    def __init__(self, existing_objects=()):
        self.objects = set(existing_objects)
        self.deleted = []

    def list_blobs(self, prefix):
        return [
            SimpleNamespace(name=name)
            for name in self.objects
            if name.startswith(prefix)
        ]

    def delete_blobs(self, blobs):
        for blob in blobs:
            self.objects.discard(blob.name)
            self.deleted.append(blob.name)


def _patch_gcs(monkeypatch, existing_objects=()):
    uploads: list[dict] = []
    bucket = FakeBucket(existing_objects)

    def upload(**kw):
        uploads.append(kw)
        bucket.objects.add(kw["object_name"])

    fake_hook = SimpleNamespace(
        upload=upload, get_conn=lambda: SimpleNamespace(bucket=lambda name: bucket)
    )
    monkeypatch.setattr(cb_operator, "GCSHook", lambda **kwargs: fake_hook)
    return uploads, bucket


def _operator(**overrides) -> ChargebackhitToGCSOperator:
    kwargs = dict(
        task_id="extract_alerts",
        account_name="acme",
        public_key_env="CHARGEBACKHIT_ACME_API_PUBLIC_KEY",
        secret_key_env="CHARGEBACKHIT_ACME_API_SECRET_KEY",
        endpoint="alerts",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )
    kwargs.update(overrides)
    return ChargebackhitToGCSOperator(**kwargs)


def _context(*, start, end, conf=None):
    return {
        "data_interval_start": start,
        "data_interval_end": end,
        "dag_run": SimpleNamespace(conf=conf or {}),
    }


def _avro_records(uploads, object_name):
    upload = next(u for u in uploads if u["object_name"] == object_name)
    return list(fastavro.reader(io.BytesIO(upload["data"])))


ONE_ALERT_PAGE = [[{"id": "a1", "object": "alert", "livemode": True}]]


class TestDateWindowMath:
    def test_single_day_scheduled_run_processes_exactly_the_data_interval(
        self, monkeypatch
    ):
        hook = _patch_chargebackhit_hook(monkeypatch, {})
        start = datetime(2026, 5, 10, 1, 0, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, 1, 0, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        hook._pages_by_window = {window_key: ONE_ALERT_PAGE}
        _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        assert (
            result["release_path"]
            == "release/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00001.avro"
        )
        assert hook.get_raw_pages_calls == [("alerts", *window_key)]

    def test_manual_trigger_same_instant_processes_previous_day_and_files_under_the_data_day(
        self, monkeypatch
    ):
        now = datetime(2026, 8, 18, 14, 30, 5, tzinfo=timezone.utc)
        yesterday = now - timedelta(days=1)
        window_key = (int(yesterday.timestamp()), int(now.timestamp()))
        hook = _patch_chargebackhit_hook(monkeypatch, {window_key: ONE_ALERT_PAGE})
        _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=now, end=now))

        assert (
            result["release_path"]
            == "release/chargebackhit/alerts/acme/2026-08-17/acme__alerts_00001.avro"
        )
        assert hook.get_raw_pages_calls == [("alerts", *window_key)]

    def test_multi_day_backfill_processes_one_window_per_day(self, monkeypatch):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 4, tzinfo=timezone.utc)  # 3 days: 1, 2, 3
        pages_by_window = {}
        current = start
        while current < end:
            nxt = current + timedelta(days=1)
            pages_by_window[(int(current.timestamp()), int(nxt.timestamp()))] = (
                ONE_ALERT_PAGE
            )
            current = nxt
        hook = _patch_chargebackhit_hook(monkeypatch, pages_by_window)
        uploads, _bucket = _patch_gcs(monkeypatch)

        trigger_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
        op = _operator(max_workers=4)
        result = op.execute(
            _context(
                start=trigger_time,
                end=trigger_time,
                conf={"date_from": "2026-01-01", "date_to": "2026-01-04"},
            )
        )

        assert (
            "release_path" not in result
        )  # only set when there's exactly one release path
        assert sorted(result["release_paths"]) == [
            "release/chargebackhit/alerts/acme/2026-01-01/acme__alerts_00001.avro",
            "release/chargebackhit/alerts/acme/2026-01-02/acme__alerts_00001.avro",
            "release/chargebackhit/alerts/acme/2026-01-03/acme__alerts_00001.avro",
        ]
        assert len(hook.get_raw_pages_calls) == 3
        landing_paths = sorted(
            u["object_name"] for u in uploads if "landing/" in u["object_name"]
        )
        assert landing_paths == [
            "landing/chargebackhit/alerts/acme/2026-01-01/acme__alerts_00001.json",
            "landing/chargebackhit/alerts/acme/2026-01-02/acme__alerts_00001.json",
            "landing/chargebackhit/alerts/acme/2026-01-03/acme__alerts_00001.json",
        ]

    def test_two_same_day_backfills_with_different_windows_do_not_collide_on_release_path(
        self, monkeypatch
    ):
        """Each day's release folder is keyed by that day (conf['date_from']/['date_to']),
        never by the DAG run's trigger data_interval plus a fixed filename suffix -- so two
        backfills manually triggered on the same calendar day for different historical
        windows land in entirely different folders (previously a pinned bug).
        """
        window_a = (
            int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp()),
            int(datetime(2025, 1, 2, tzinfo=timezone.utc).timestamp()),
        )
        window_b = (
            int(datetime(2025, 6, 1, tzinfo=timezone.utc).timestamp()),
            int(datetime(2025, 6, 2, tzinfo=timezone.utc).timestamp()),
        )
        _patch_chargebackhit_hook(
            monkeypatch, {window_a: ONE_ALERT_PAGE, window_b: ONE_ALERT_PAGE}
        )
        _patch_gcs(monkeypatch)
        trigger_time = datetime(2026, 8, 18, tzinfo=timezone.utc)

        op_a = _operator()
        result_a = op_a.execute(
            _context(
                start=trigger_time,
                end=trigger_time,
                conf={"date_from": "2025-01-01", "date_to": "2025-01-02"},
            )
        )

        op_b = _operator()
        result_b = op_b.execute(
            _context(
                start=trigger_time,
                end=trigger_time,
                conf={"date_from": "2025-06-01", "date_to": "2025-06-02"},
            )
        )

        assert result_a["release_path"] != result_b["release_path"]

    def test_date_to_is_exclusive(self, monkeypatch):
        _patch_chargebackhit_hook(monkeypatch, {})
        uploads, _bucket = _patch_gcs(monkeypatch)
        trigger_time = datetime(2026, 8, 18, tzinfo=timezone.utc)

        op = _operator()
        op.execute(
            _context(
                start=trigger_time,
                end=trigger_time,
                conf={"date_from": "2026-01-01", "date_to": "2026-01-01"},
            )
        )

        # date_from == date_to -> zero days to process, no landing uploads at all.
        landing_calls = [c for c in uploads if c["object_name"].startswith("landing/")]
        assert landing_calls == []


class TestAllowlistFiltering:
    def test_endpoint_not_in_allowlist_returns_empty_result_instead_of_skipping(
        self, monkeypatch
    ):
        """Returning an empty result (rather than raising AirflowSkipException) keeps this
        mapped instance's XCom populated -- the DAG's downstream collect_release_paths task
        reads every mapped instance's .output, and a skipped instance pushes no XCom at all,
        which can crash that collection step."""
        hook = _patch_chargebackhit_hook(monkeypatch, {})
        _patch_gcs(monkeypatch)
        op = _operator(endpoint="alerts")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
            conf={"endpoints": ["disputes"]},
        )

        result = op.execute(ctx)

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }
        assert hook.get_raw_pages_calls == []

    def test_account_not_in_allowlist_returns_empty_result_instead_of_skipping(
        self, monkeypatch
    ):
        hook = _patch_chargebackhit_hook(monkeypatch, {})
        _patch_gcs(monkeypatch)
        op = _operator(account_name="acme")
        ctx = _context(
            start=datetime(2026, 5, 10, tzinfo=timezone.utc),
            end=datetime(2026, 5, 11, tzinfo=timezone.utc),
            conf={"accounts": ["someone_else"]},
        )

        result = op.execute(ctx)

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }
        assert hook.get_raw_pages_calls == []

    def test_account_allowlist_match_is_case_insensitive(self, monkeypatch):
        start = datetime(2026, 5, 10, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        hook = _patch_chargebackhit_hook(monkeypatch, {window_key: ONE_ALERT_PAGE})  # noqa: F841
        _patch_gcs(monkeypatch)
        op = _operator(account_name="acme")
        ctx = _context(start=start, end=end, conf={"accounts": ["ACME"]})

        result = op.execute(ctx)

        assert result["landing_pages_processed"] == 1


class TestFileNaming:
    def test_account_name_is_lowercased_and_account_entity_are_double_underscore_separated(
        self, monkeypatch
    ):
        start = datetime(2026, 5, 10, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        _patch_chargebackhit_hook(monkeypatch, {window_key: ONE_ALERT_PAGE})
        _patch_gcs(monkeypatch)
        op = _operator(account_name="ACME")

        result = op.execute(_context(start=start, end=end))

        assert (
            result["release_path"]
            == "release/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00001.avro"
        )


class TestRecordShapingAndAlwaysUploadingReleaseAvro:
    def test_metadata_noise_keys_are_stripped_from_release_records_but_not_landing(
        self, monkeypatch
    ):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        pages = [[{"id": "a1", "object": "alert", "livemode": True, "outcome": "won"}]]
        _patch_chargebackhit_hook(monkeypatch, {window_key: pages})
        uploads, _bucket = _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        landing_call = next(
            u for u in uploads if u["object_name"].startswith("landing/")
        )
        # Landing is a lossless copy: object/livemode are still present there.
        landed = json.loads(landing_call["data"])
        assert landed == [
            {"id": "a1", "object": "alert", "livemode": True, "outcome": "won"}
        ]

        raw_records = [
            json.loads(r["raw_data"])
            for r in _avro_records(uploads, result["release_path"])
        ]
        # But the Release record's raw_data has "object"/"livemode" stripped out.
        assert raw_records == [{"id": "a1", "outcome": "won"}]

    def test_one_avro_row_per_record_not_per_page(self, monkeypatch):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        pages = [[{"id": "a1"}, {"id": "a2"}]]
        _patch_chargebackhit_hook(monkeypatch, {window_key: pages})
        uploads, _bucket = _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        raw_records = [
            json.loads(r["raw_data"])
            for r in _avro_records(uploads, result["release_path"])
        ]
        assert raw_records == [{"id": "a1"}, {"id": "a2"}]

    def test_zero_records_still_writes_and_uploads_a_release_avro(self, monkeypatch):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        _patch_chargebackhit_hook(
            monkeypatch, {window_key: [[]]}
        )  # one page, zero records
        uploads, _bucket = _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        assert result["landing_pages_processed"] == 1
        release_upload = next(
            u for u in uploads if u["object_name"] == result["release_path"]
        )
        assert release_upload["mime_type"] == "associated-avro-binary"

    def test_a_day_with_zero_pages_yields_no_release_files(self, monkeypatch):
        _patch_chargebackhit_hook(monkeypatch, {})  # no pages for this window
        _patch_gcs(monkeypatch)
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        assert result == {
            "landing_pages_processed": 0,
            "total_records": 0,
            "release_paths": [],
        }

    def test_each_page_is_written_as_its_own_landing_and_release_file(
        self, monkeypatch
    ):
        start = datetime(2026, 5, 10, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        pages = [[{"id": "a1"}], [{"id": "a2"}]]
        _patch_chargebackhit_hook(monkeypatch, {window_key: pages})
        uploads, _bucket = _patch_gcs(monkeypatch)

        op = _operator()
        result = op.execute(_context(start=start, end=end))

        assert sorted(result["release_paths"]) == [
            "release/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00001.avro",
            "release/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00002.avro",
        ]
        landing_paths = sorted(
            u["object_name"] for u in uploads if "landing/" in u["object_name"]
        )
        assert landing_paths == [
            "landing/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00001.json",
            "landing/chargebackhit/alerts/acme/2026-05-10/acme__alerts_00002.json",
        ]
        first_page_records = [
            json.loads(r["raw_data"])
            for r in _avro_records(uploads, result["release_paths"][0])
        ]
        assert first_page_records == [{"id": "a1"}]


class TestStaleFileCleanup:
    def test_prior_run_leftover_page_files_are_deleted_before_this_run_writes(
        self, monkeypatch
    ):
        """Simulates a rerun where a previous execution wrote 2 pages for this account+day
        but this run only produces 1. Without cleanup, page 2 would linger and get
        double-counted by the BigQuery load."""
        start = datetime(2026, 5, 10, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        _patch_chargebackhit_hook(monkeypatch, {window_key: ONE_ALERT_PAGE})
        stale_landing = (
            "landing/chargebackhit/alerts/acme/2026-05-10/acme__alerts__00002.json"
        )
        stale_release = (
            "release/chargebackhit/alerts/acme/2026-05-10/acme__alerts__00002.avro"
        )
        other_account_release = (
            "release/chargebackhit/alerts/other/2026-05-10/other__alerts__00001.avro"
        )
        uploads, bucket = _patch_gcs(
            monkeypatch,
            existing_objects=[stale_landing, stale_release, other_account_release],
        )

        op = _operator()
        op.execute(_context(start=start, end=end))

        assert stale_landing in bucket.deleted
        assert stale_release in bucket.deleted
        # Cleanup must be scoped to this account's own directory -- it must never touch
        # another account's files, even though they share the same endpoint/day.
        assert other_account_release not in bucket.deleted
        assert other_account_release in bucket.objects

    def test_cleanup_is_a_noop_when_nothing_stale_exists(self, monkeypatch):
        start = datetime(2026, 5, 10, tzinfo=timezone.utc)
        end = datetime(2026, 5, 11, tzinfo=timezone.utc)
        window_key = (int(start.timestamp()), int(end.timestamp()))
        _patch_chargebackhit_hook(monkeypatch, {window_key: ONE_ALERT_PAGE})
        uploads, bucket = _patch_gcs(monkeypatch)

        op = _operator()
        op.execute(_context(start=start, end=end))

        assert bucket.deleted == []
