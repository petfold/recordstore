"""R0 of the local-first track: both in-memory caches are bounded, and the
value-blob cache exists at all (before this, every re-read of a committed
record was a fresh backend fetch).

Correctness must never depend on cache size — several tests run with
hostile bounds (smaller than one commit's working set) and assert results
against a plain dict oracle.
"""

import pytest

from recordstore import CachedBytesStore, MemoryBytesStore, RecordStore


class CountingStore(MemoryBytesStore):
    def __init__(self):
        super().__init__()
        self.gets = 0

    def get(self, ref):
        self.gets += 1
        return super().get(ref)


# -- CachedBytesStore ---------------------------------------------------------


def test_repeat_reads_served_from_memory():
    inner = CountingStore()
    store = RecordStore(CachedBytesStore(inner))
    store.put("k", {"big": "value"})
    store.commit()
    first = inner.gets
    for _ in range(10):
        assert store.get("k") == {"big": "value"}
    assert inner.gets == first  # value blob + nodes all cached


def test_cache_is_byte_bounded_lru():
    inner = CountingStore()
    cache = CachedBytesStore(inner, max_bytes=250)
    r1, r2, r3 = (cache.put(bytes([i]) * 100) for i in range(3))
    assert cache._bytes <= 250
    cache.get(r1)                 # evicted -> falls through to inner
    assert inner.gets == 1
    cache.get(r3)                 # recent -> still cached
    assert inner.gets == 1


def test_oversized_blob_served_but_never_cached():
    inner = CountingStore()
    cache = CachedBytesStore(inner, max_bytes=50)
    ref = cache.put(b"y" * 200)
    assert cache.get(ref) == b"y" * 200
    assert inner.gets == 1 and cache._bytes == 0


def test_get_many_mixes_cache_and_backend():
    inner = CountingStore()
    cache = CachedBytesStore(inner, max_bytes=10_000)
    r1 = cache.put(b"a" * 10)
    r2 = inner.put(b"b" * 10)     # only the backend knows this one
    assert cache.get_many([r1, r2]) == {r1: b"a" * 10, r2: b"b" * 10}
    assert inner.gets == 1


def test_unknown_attributes_delegate_to_inner():
    inner = CountingStore()
    inner.frobnicate = lambda: "ok"
    assert CachedBytesStore(inner).frobnicate() == "ok"


def test_store_correct_through_cache_wrapper():
    oracle = {}
    store = RecordStore(CachedBytesStore(MemoryBytesStore(), max_bytes=300))
    for i in range(50):
        store.put(f"k{i}", {"i": i})
        oracle[f"k{i}"] = {"i": i}
    store.commit()
    assert dict(store.items()) == oracle


# -- bounded trie-node cache ------------------------------------------------------


def test_node_cache_is_bounded_after_full_iteration():
    store = RecordStore(MemoryBytesStore(), node_cache_size=32)
    for i in range(500):
        store.put(f"key/{i:04d}", i)
    store.commit()
    assert dict(store.items()) == {f"key/{i:04d}": i for i in range(500)}
    assert len(store._trie._cache) <= 32


def test_hostile_cache_smaller_than_one_commit():
    """Pending placeholders are exempt from eviction, so a commit whose
    buffered node set dwarfs the cache bound still lands correctly."""
    store = RecordStore(MemoryBytesStore(), node_cache_size=2)
    oracle = {}
    for i in range(200):
        store.put(f"deep/nested/key/{i:03d}", {"v": i})
        oracle[f"deep/nested/key/{i:03d}"] = {"v": i}
    store.commit()
    assert dict(store.items()) == oracle
    for k, v in oracle.items():
        assert store.get(k) == v


def test_tiny_cache_random_ops_match_oracle():
    import random
    rng = random.Random(42)
    store = RecordStore(MemoryBytesStore(), node_cache_size=4)
    oracle = {}
    for step in range(300):
        k = f"k{rng.randint(0, 60)}"
        if rng.random() < 0.7:
            store.put(k, step)
            oracle[k] = step
        elif k in oracle:
            store.delete(k)
            del oracle[k]
        if rng.random() < 0.2:
            store.commit()
    store.commit()
    assert dict(store.items()) == oracle


def test_merge_unaffected_by_small_cache():
    blobs = MemoryBytesStore()
    base_store = RecordStore(blobs, node_cache_size=4)
    for i in range(30):
        base_store.put(f"k{i}", i)
    base = base_store.commit()

    ours_store = RecordStore(blobs, root=base, node_cache_size=4)
    ours_store.put("k1", "ours")
    ours = ours_store.commit()

    theirs_store = RecordStore(blobs, root=base, node_cache_size=4)
    theirs_store.put("k2", "theirs")
    theirs = theirs_store.commit()

    merged = RecordStore.merge(blobs, base, ours, theirs)
    m = RecordStore.at(merged, blobs)
    assert m.get("k1") == "ours" and m.get("k2") == "theirs"
