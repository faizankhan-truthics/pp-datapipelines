"""Unit tests for each Solidgate hook's ``_extract_records`` (the per-endpoint payload shape).

Every hook must handle both the flat top-level shape and the ``{"body": {...}}``-wrapped
shape the Solidgate API sometimes returns, plus a payload with no records at all
(must yield ``[]``, not raise/None) so ``land_and_release_to_gcs`` can always iterate it.
"""

import pytest

from include.custom_hooks.solidgate.apm_orders import ApmOrdersHook
from include.custom_hooks.solidgate.card_orders import CardOrdersHook
from include.custom_hooks.solidgate.chargebacks import ChargebacksHook
from include.custom_hooks.solidgate.fraud_alerts import FraudAlertsHook
from include.custom_hooks.solidgate.subscriptions import SubscriptionsHook

RECORD = {"id": "r1"}


@pytest.mark.parametrize(
    "hook_cls,top_level_key,nested_key",
    [
        (CardOrdersHook, "orders", "orders"),
        (ApmOrdersHook, "orders", "orders"),
        (ChargebacksHook, "orders", "chargebacks"),
    ],
)
def test_orders_shaped_hooks_extract_records(hook_cls, top_level_key, nested_key):
    assert hook_cls._extract_records({top_level_key: [RECORD]}) == [RECORD]
    assert hook_cls._extract_records({"body": {nested_key: [RECORD]}}) == [RECORD]
    assert hook_cls._extract_records({}) == []
    assert hook_cls._extract_records({top_level_key: None}) == []


def test_fraud_alerts_hook_uses_alerts_key_and_nested_fraud_alerts_key():
    assert FraudAlertsHook._extract_records({"alerts": [RECORD]}) == [RECORD]
    assert FraudAlertsHook._extract_records({"body": {"fraud_alerts": [RECORD]}}) == [
        RECORD
    ]
    assert FraudAlertsHook._extract_records({}) == []


class TestSubscriptionsHookRecordExtraction:
    def test_unwraps_uuid_keyed_dict_into_a_list_of_values(self):
        payload = {"subscriptions": {"uuid-a": {"id": "a"}, "uuid-b": {"id": "b"}}}

        records = SubscriptionsHook._extract_records(payload)

        assert sorted(records, key=lambda r: r["id"]) == [{"id": "a"}, {"id": "b"}]

    def test_nested_body_dict_shape_is_also_unwrapped(self):
        payload = {"body": {"subscriptions": {"uuid-a": {"id": "a"}}}}

        assert SubscriptionsHook._extract_records(payload) == [{"id": "a"}]

    def test_plain_list_shape_passes_through_unchanged(self):
        assert SubscriptionsHook._extract_records({"subscriptions": [RECORD]}) == [
            RECORD
        ]

    def test_missing_node_returns_empty_list(self):
        assert SubscriptionsHook._extract_records({}) == []


OTHER = {"id": "r2"}

# hook -> (flat key, body-wrapped key)
ALL_PAGINATED_HOOKS = {
    CardOrdersHook: ("orders", "orders"),
    ApmOrdersHook: ("orders", "orders"),
    ChargebacksHook: ("orders", "chargebacks"),
    FraudAlertsHook: ("alerts", "fraud_alerts"),
    SubscriptionsHook: ("subscriptions", "subscriptions"),
}


@pytest.mark.parametrize("hook_cls", ALL_PAGINATED_HOOKS, ids=lambda h: h.__name__)
class TestEdgeShapesForEveryHook:
    def test_null_body_wrapped_node_returns_empty_list(self, hook_cls):
        _flat, nested = ALL_PAGINATED_HOOKS[hook_cls]
        assert hook_cls._extract_records({"body": {nested: None}}) == []

    def test_null_flat_node_returns_empty_list(self, hook_cls):
        flat, _nested = ALL_PAGINATED_HOOKS[hook_cls]
        assert hook_cls._extract_records({flat: None}) == []

    def test_body_without_the_records_key_returns_empty_list(self, hook_cls):
        assert (
            hook_cls._extract_records(
                {"body": {"metadata": {"next_page_iterator": None}}}
            )
            == []
        )

    def test_flat_shape_takes_precedence_over_body_wrapped_shape(self, hook_cls):
        flat, nested = ALL_PAGINATED_HOOKS[hook_cls]
        payload = {flat: [RECORD], "body": {nested: [OTHER]}}

        assert hook_cls._extract_records(payload) == [RECORD]

    def test_multiple_records_keep_their_order(self, hook_cls):
        flat, _nested = ALL_PAGINATED_HOOKS[hook_cls]
        assert hook_cls._extract_records({flat: [RECORD, OTHER]}) == [RECORD, OTHER]


def test_chargebacks_flat_shape_is_read_from_orders_not_chargebacks():
    # The flat /card-orders/chargebacks response lists orders (each with a nested
    # chargebacks array), which is what merge_chargebacks.sql shreds on order_id.
    assert ChargebacksHook._extract_records({"chargebacks": [RECORD]}) == []


def test_subscriptions_empty_uuid_map_returns_empty_list():
    assert SubscriptionsHook._extract_records({"subscriptions": {}}) == []
    assert SubscriptionsHook._extract_records({"body": {"subscriptions": {}}}) == []
