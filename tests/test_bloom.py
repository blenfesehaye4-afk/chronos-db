import pytest
from chronos_db.bloom import BloomFilter


def test_bloom_membership_and_zero_false_negatives():
    bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    keys = [f"user:{i}" for i in range(500)]

    for k in keys:
        bf.add(k)

    # Must have 0 false negatives
    for k in keys:
        assert bf.contains(k) is True

    # Check non-existent keys (should have very low false positive rate)
    false_positives = sum(1 for i in range(500, 1500) if bf.contains(f"user:{i}"))
    fp_rate = false_positives / 1000
    assert fp_rate < 0.05


def test_bloom_serialization_roundtrip():
    bf = BloomFilter(expected_elements=200, false_positive_rate=0.02)
    for w in ["apple", "banana", "cherry", "durian"]:
        bf.add(w)

    data = bf.to_bytes()
    restored = BloomFilter.from_bytes(data)

    assert restored.contains("apple") is True
    assert restored.contains("banana") is True
    assert restored.contains("mango") is False
