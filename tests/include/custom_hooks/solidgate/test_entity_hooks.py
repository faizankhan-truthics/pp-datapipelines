"""Per-entity tests for the five paginated Solidgate hooks (include/custom_hooks/solidgate/*.py).

The shared base.py pagination/signing tests only ever drive ``CardOrdersHook``, and
test_extract_records.py calls each ``_extract_records`` in isolation. Neither checks
that each entity hook, end to end, (a) hits its own endpoint with its own page size,
(b) follows pagination using its own payload shape (flat and ``{"body": ...}``-wrapped),
and (c) lands every record, with its business key, in the Release Avro ``raw_data``
that the entity's merge_*.sql dedups on. A wrong top-level key in one hook makes
``_extract_records`` return ``[]``, which is indistinguishable from a quiet day: every
page still lands, the Avro is written empty, and bronze just never gets the rows.

financial_entries is report-based (no ``_extract_records``); see
test_financial_entries_hook.py.
"""

from __future__ import annotations

import io
import json
from types import SimpleNamespace

import pytest
from fastavro import reader as avro_reader

import include.custom_operators.solidgate.solidgate_to_gcs_operator as sg_operator
from include.custom_hooks.solidgate.apm_orders import ApmOrdersHook
from include.custom_hooks.solidgate.card_orders import CardOrdersHook
from include.custom_hooks.solidgate.chargebacks import ChargebacksHook
from include.custom_hooks.solidgate.fraud_alerts import FraudAlertsHook
from include.custom_hooks.solidgate.subscriptions import SubscriptionsHook
from include.custom_operators.solidgate.solidgate_to_gcs_operator import (
    land_and_release_to_gcs,
)


def _as_list(records):
    return records


def _as_uuid_map(records):
    # /subscriptions returns {uuid: record} rather than a list.
    return {f"uuid-{r['id']}": r for r in records}


# entity -> hook, endpoint, page limit, flat key, body-wrapped key, merge-SQL business key, record container
ENTITIES = {
    "apm_orders": (
        ApmOrdersHook,
        "/apm-orders",
        2000,
        "orders",
        "orders",
        "order_id",
        _as_list,
    ),
    "card_orders": (
        CardOrdersHook,
        "/card-orders",
        2000,
        "orders",
        "orders",
        "order_id",
        _as_list,
    ),
    "chargebacks": (
        ChargebacksHook,
        "/card-orders/chargebacks",
        2000,
        "orders",
        "chargebacks",
        "order_id",
        _as_list,
    ),
    "fraud_alerts": (
        FraudAlertsHook,
        "/card-orders/fraud-alerts",
        2000,
        "alerts",
        "fraud_alerts",
        "id",
        _as_list,
    ),
    "subscriptions": (
        SubscriptionsHook,
        "/subscriptions",
        200,
        "subscriptions",
        "subscriptions",
        "id",
        _as_uuid_map,
    ),
}


@pytest.fixture(params=sorted(ENTITIES))
def entity(request):
    return request.param


def _spec(entity):
    hook_cls, path, limit, flat_key, body_key, business_key, container = ENTITIES[
        entity
    ]
    return SimpleNamespace(
        hook_cls=hook_cls,
        path=path,
        limit=limit,
        flat_key=flat_key,
        body_key=body_key,
        business_key=business_key,
        container=container,
    )


def _records(spec, *ids):
    return [{spec.business_key: i, "status": "settled"} for i in ids]


def _two_page_api(spec):
    """Page 1 in the flat shape, page 2 in the body-wrapped shape (with nested metadata)."""
    return [
        {
            spec.flat_key: spec.container(_records(spec, "r1", "r2")),
            "metadata": {"next_page_iterator": "c1"},
        },
        {
            "body": {
                spec.body_key: spec.container(_records(spec, "r3")),
                "metadata": {"next_page_iterator": None},
            }
        },
    ]


def _stub_api(monkeypatch, hook, pages):
    requests_made = []

    def fake_signed_request(method, path, *, body=""):
        requests_made.append((method, path, json.loads(body)))
        payload = pages[len(requests_made) - 1]
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)

    monkeypatch.setattr(hook, "signed_request", fake_signed_request)
    return requests_made


def _hook(spec):
    return spec.hook_cls(public_key_env="TEST_PUBLIC", secret_key_env="TEST_SECRET")


class TestEndpointAndPaging:
    def test_posts_to_this_entitys_endpoint_with_its_page_limit(
        self, entity, monkeypatch
    ):
        spec = _spec(entity)
        hook = _hook(spec)
        requests_made = _stub_api(monkeypatch, hook, _two_page_api(spec))

        list(hook.iter_raw_pages("2026-01-01 00:00:00", "2026-01-02 00:00:00"))

        assert [(m, p) for m, p, _ in requests_made] == [("POST", spec.path)] * 2
        first_body = requests_made[0][2]
        assert first_body == {
            "filter": "updated_at",
            "date_from": "2026-01-01 00:00:00",
            "date_to": "2026-01-02 00:00:00",
            "limit": spec.limit,
        }
        assert requests_made[1][2]["next_page_iterator"] == "c1"

    def test_subscriptions_uses_a_smaller_page_than_the_other_entities(self):
        # ~4 KB records: a 2000-row page exceeds the read timeout.
        assert SubscriptionsHook._PAGE_LIMIT == 200
        assert {
            h._PAGE_LIMIT
            for h in (ApmOrdersHook, CardOrdersHook, ChargebacksHook, FraudAlertsHook)
        } == {2000}


class TestRecordExtractionAcrossPages:
    def test_iter_records_returns_every_record_from_both_payload_shapes(
        self, entity, monkeypatch
    ):
        spec = _spec(entity)
        hook = _hook(spec)
        _stub_api(monkeypatch, hook, _two_page_api(spec))

        pages = list(hook.iter_records("2026-01-01 00:00:00", "2026-01-02 00:00:00"))

        assert pages == [_records(spec, "r1", "r2"), _records(spec, "r3")]

    def test_empty_page_mid_stream_does_not_stop_pagination(self, entity, monkeypatch):
        spec = _spec(entity)
        hook = _hook(spec)
        pages = [
            {
                spec.flat_key: spec.container([]),
                "metadata": {"next_page_iterator": "c1"},
            },
            {spec.flat_key: spec.container(_records(spec, "r1")), "metadata": {}},
        ]
        _stub_api(monkeypatch, hook, pages)

        assert list(
            hook.iter_records("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        ) == [[], _records(spec, "r1")]

    def test_payload_keyed_for_another_entity_yields_no_records(self, entity):
        # Guards the "wrong key looks like a quiet day" failure: a hook must not pick up
        # records from a key it doesn't own.
        spec = _spec(entity)
        foreign_key = "unrelated_entities"
        assert spec.hook_cls._extract_records({foreign_key: _records(spec, "r1")}) == []


class _MemBlob:
    def __init__(self):
        self.data = b""

    def upload_from_string(self, data, content_type=None):
        self.data = data if isinstance(data, bytes) else data.encode("utf-8")


class _MemBucket:
    def __init__(self):
        self.blobs: dict[str, _MemBlob] = {}

    def blob(self, path):
        return self.blobs.setdefault(path, _MemBlob())

    def list_blobs(self, prefix):
        return [SimpleNamespace(name=n) for n in self.blobs if n.startswith(prefix)]

    def delete_blobs(self, blobs):
        for b in blobs:
            self.blobs.pop(b.name, None)


class TestRecordsReachTheReleaseAvro:
    def test_every_record_lands_in_release_raw_data_with_its_business_key(
        self, entity, monkeypatch
    ):
        spec = _spec(entity)
        hook = _hook(spec)
        _stub_api(monkeypatch, hook, _two_page_api(spec))
        bucket = _MemBucket()
        monkeypatch.setattr(
            sg_operator,
            "GCSHook",
            lambda **kw: SimpleNamespace(
                get_conn=lambda: SimpleNamespace(bucket=lambda name: bucket)
            ),
        )

        result = land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-02 00:00:00",
            entity=entity,
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 3
        assert result["landing_pages_processed"] == 2
        released = [
            row
            for path in result["release_paths"]
            for row in avro_reader(io.BytesIO(bucket.blobs[path].data))
        ]
        assert sorted(
            json.loads(r["raw_data"])[spec.business_key] for r in released
        ) == ["r1", "r2", "r3"]
        assert {r["_elt_endpoint"] for r in released} == {entity}
        assert {r["_elt_account_name"] for r in released} == {"acme"}
