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
    def __init__(self, **kwargs):
        self.kwargs = kwargs
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
    fake_gcs = FakeGCSHook()
    created_hooks = []

    def hook_factory(**kwargs):
        hook = FakePaypalHook(**kwargs)
        created_hooks.append(hook)
        return hook

    monkeypatch.setattr(paypal_operator, "PaypalHook", hook_factory)
    monkeypatch.setattr(paypal_operator, "GCSHook", lambda **_kwargs: fake_gcs)

    operator = PaypalToGCSOperator(
        task_id="extract_transactions",
        account_name="sandbox",
        environment="sandbox",
        client_id_env="PAYPAL_CLIENT_ID_SANDBOX",
        client_secret_env="PAYPAL_CLIENT_SECRET_SANDBOX",
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

    assert created_hooks[0].windows == [
        ("2026-01-01T00:00:00.000Z", "2026-01-01T23:59:59.999Z")
    ]
    assert result["total_records"] == 1
    assert created_hooks[0].kwargs["environment"] == "sandbox"
    assert result["release_paths"] == [
        "release/paypal/transactions/sandbox/2026-01-01/sandbox_transactions_00001.avro"
    ]

    landing = next(
        upload
        for upload in fake_gcs.uploads
        if upload["mime_type"] == "application/json"
    )
    assert landing["object_name"].startswith("landing/paypal/transactions/")

    release = next(
        upload
        for upload in fake_gcs.uploads
        if upload["mime_type"] == "associated-avro-binary"
    )
    records = list(reader(io.BytesIO(release["data"])))
    assert records[0]["_elt_account_name"] == "sandbox"
    assert records[0]["_elt_environment"] == "sandbox"
    assert records[0]["_elt_endpoint"] == "transactions"
    assert '"transaction_id": "TX-1"' in records[0]["raw_data"]


def test_operator_uses_configured_account_name_in_gcs_paths(monkeypatch):
    fake_gcs = FakeGCSHook()
    monkeypatch.setattr(
        paypal_operator,
        "PaypalHook",
        lambda **kwargs: FakePaypalHook(**kwargs),
    )
    monkeypatch.setattr(paypal_operator, "GCSHook", lambda **_kwargs: fake_gcs)
    operator = PaypalToGCSOperator(
        task_id="extract_transactions_prod",
        account_name="payments_prod",
        environment="production",
        client_id_env="PAYPAL_CLIENT_ID_PROD",
        client_secret_env="PAYPAL_CLIENT_SECRET_PROD",
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

    assert result["release_paths"][0].startswith(
        "release/paypal/transactions/payments_prod/"
    )
