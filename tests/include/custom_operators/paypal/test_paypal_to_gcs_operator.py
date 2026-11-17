from __future__ import annotations

import io
from datetime import datetime, timezone
from types import SimpleNamespace

from fastavro import reader

import include.custom_operators.paypal.paypal_to_gcs_operator as paypal_operator
from include.custom_operators.paypal.paypal_to_gcs_operator import (
    PaypalToGCSOperator,
)


class FakeBucket:
    def list_blobs(self, prefix):
        return []

    def delete_blobs(self, blobs):
        raise AssertionError(f"Unexpected stale blobs: {blobs}")


class FakeGCSHook:
    def __init__(self, **_kwargs):
        self.uploads = []
        self._bucket = FakeBucket()

    def get_conn(self):
        return SimpleNamespace(bucket=lambda _name: self._bucket)

    def upload(self, **kwargs):
        self.uploads.append(kwargs)


class FakePaypalHook:
    def __init__(self):
        self.windows = []

    def get_raw_pages(self, start_date, end_date):
        self.windows.append((start_date, end_date))
        yield {
            "total_pages": 1,
            "transaction_details": [
                {
                    "transaction_info": {
                        "transaction_id": "TX-1",
                        "transaction_amount": {
                            "value": "12.34",
                            "currency_code": "USD",
                        },
                    }
                }
            ],
        }


def test_operator_lands_page_and_releases_transaction_records(monkeypatch):
    fake_hook = FakePaypalHook()
    fake_gcs = FakeGCSHook()
    monkeypatch.setattr(paypal_operator, "PaypalHook", lambda: fake_hook)
    monkeypatch.setattr(paypal_operator, "GCSHook", lambda **_kwargs: fake_gcs)

    operator = PaypalToGCSOperator(
        task_id="extract_transactions",
        gcs_bucket="test-bucket",
    )
    result = operator.execute(
        {
            "params": {"date_from": "", "date_to": ""},
            "data_interval_start": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "data_interval_end": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "dag_run": SimpleNamespace(conf={}),
        }
    )

    assert fake_hook.windows == [
        ("2026-01-01T00:00:00.000Z", "2026-01-01T23:59:59.999Z")
    ]
    assert result["total_records"] == 1
    assert result["release_paths"] == [
        "release/paypal/transactions/paypal_sandbox/2026-01-01/"
        "paypal_sandbox_transactions_00001.avro"
    ]

    landing = next(
        upload for upload in fake_gcs.uploads if upload["mime_type"] == "application/json"
    )
    assert landing["object_name"].startswith("landing/paypal/transactions/")

    release = next(
        upload
        for upload in fake_gcs.uploads
        if upload["mime_type"] == "associated-avro-binary"
    )
    records = list(reader(io.BytesIO(release["data"])))
    assert records[0]["_elt_account_name"] == "paypal_sandbox"
    assert records[0]["_elt_endpoint"] == "transactions"
    assert '"transaction_id": "TX-1"' in records[0]["raw_data"]
