"""
Tests for rtm_kafka/producer/event_generator.py

Verifies that generate_event() produces the correct schema, field types,
and that bad-data injection rates are statistically within expected bounds.
"""

import pytest
from rtm_kafka.producer.event_generator import generate_event
from config.constants import (
    BAD_DATA_RATE_EVENT_ID,
    BAD_DATA_RATE_QUANTITY,
    BAD_DATA_RATE_PRICE,
    ALL_DIVISIONS,
)


EXPECTED_KEYS = {
    "event_id", "store_id", "product_id", "division",
    "quantity", "unit_price", "coupon_applied", "event_timestamp",
}


def test_event_schema_keys():
    event = generate_event(0)
    assert set(event.keys()) == EXPECTED_KEYS


def test_event_id_format():
    for i in [0, 1, 999999]:
        event = generate_event(i)
        if event["event_id"] is not None:
            assert event["event_id"] == f"EVT{i:06d}"


def test_store_id_format():
    for _ in range(100):
        event = generate_event(0)
        assert event["store_id"].startswith("STORE")


def test_product_id_format():
    for _ in range(100):
        event = generate_event(0)
        assert event["product_id"].startswith("PROD")


def test_division_in_allowed_values():
    for _ in range(200):
        event = generate_event(0)
        assert event["division"] in ALL_DIVISIONS


def test_coupon_applied_is_bool():
    for _ in range(50):
        event = generate_event(0)
        assert isinstance(event["coupon_applied"], bool)


def test_event_timestamp_is_string():
    event = generate_event(0)
    assert isinstance(event["event_timestamp"], str)
    assert "T" in event["event_timestamp"]   # ISO format check


def test_all_fields_correct_types():
    for _ in range(50):
        event = generate_event(42)
        assert event["event_id"] is None or isinstance(event["event_id"], str)
        assert isinstance(event["store_id"], str)
        assert isinstance(event["product_id"], str)
        assert event["division"] is None or isinstance(event["division"], str)
        assert isinstance(event["quantity"], int)
        assert isinstance(event["unit_price"], float)
        assert isinstance(event["coupon_applied"], bool)
        assert isinstance(event["event_timestamp"], str)


def test_bad_data_rates_within_bounds():
    """
    Generate 10,000 events and verify injection rates are within ±50% of their targets.
    E.g. 2% target → acceptable range 1%–3%.
    """
    n = 10_000
    tolerance = 0.5     # 50% relative tolerance

    null_event_id = sum(1 for i in range(n) if generate_event(i)["event_id"] is None)
    neg_quantity  = sum(1 for i in range(n) if generate_event(i)["quantity"] < 0)
    zero_price    = sum(1 for i in range(n) if generate_event(i)["unit_price"] == 0)

    def within_bounds(actual_rate, target_rate):
        return target_rate * (1 - tolerance) <= actual_rate <= target_rate * (1 + tolerance)

    assert within_bounds(null_event_id / n, BAD_DATA_RATE_EVENT_ID), (
        f"event_id null rate {null_event_id/n:.3f} out of bounds for target {BAD_DATA_RATE_EVENT_ID}"
    )
    assert within_bounds(neg_quantity / n, BAD_DATA_RATE_QUANTITY), (
        f"negative quantity rate {neg_quantity/n:.3f} out of bounds for target {BAD_DATA_RATE_QUANTITY}"
    )
    assert within_bounds(zero_price / n, BAD_DATA_RATE_PRICE), (
        f"zero price rate {zero_price/n:.3f} out of bounds for target {BAD_DATA_RATE_PRICE}"
    )
