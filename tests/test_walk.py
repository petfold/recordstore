"""What a full walk of the trie costs a network store.

`items()`/`keys()` and the working-set walk behind pins and squashing
read the trie depth-first, which is what streams keys in sorted order
with bounded memory. They used to load each node's children as a
separate batch, so a network store paid one round trip per trie node
with children: 2,679 rounds for a 5,380-record store, about ten minutes
against a Bee light node. They now load ahead level by level, so the
rounds follow the trie's depth. These tests count rounds (calls into the
blob store) rather than time them.

A diff (and so `merge`, and every consumer folding one root into another)
walks two tries at once and had the same one-round-per-node cost; it now
loads ahead the same way.
"""

import math
import random

from recordstore import MemoryBytesStore, RecordStore
from recordstore.recordstore import _Trie


class Counting:
    """A blob store that counts its calls: each `get_many` is one round."""

    def __init__(self, inner):
        self.inner, self.calls, self.fetched = inner, 0, 0

    def get(self, ref):
        self.calls += 1
        self.fetched += 1
        return self.inner.get(ref)

    def get_many(self, refs):
        refs = list(refs)
        self.calls += 1
        self.fetched += len(refs)
        return self.inner.get_many(refs)

    def put(self, data):
        return self.inner.put(data)

    def put_many(self, datas):
        return self.inner.put_many(datas)


def _store(n=3000, seed=7):
    rng = random.Random(seed)
    words = ["".join(rng.choice("abcdefgh") for _ in range(rng.randint(2, 6)))
             for _ in range(400)]
    keys = sorted({f"{rng.choice(words)}/{rng.choice(words)}-{i}"
                   for i in range(n)})
    blobs = MemoryBytesStore()
    store = RecordStore(blobs)
    for k in keys:
        store.put(k, {"k": k})
    return blobs, store.commit(), keys


def _shape(blobs, root):
    """Trie nodes and levels, counted from the store directly."""
    trie, level, nodes, levels = _Trie(blobs), [root], 0, 0
    while level:
        levels += 1
        nodes += len(level)
        level = [c for r in level for c in trie._load(r).children.values()]
    return nodes, levels


def test_a_full_walk_costs_rounds_per_level_not_per_node():
    blobs, root, keys = _store()
    nodes, levels = _shape(blobs, root)
    counting = Counting(blobs)
    assert list(RecordStore.at(root, counting).keys()) == keys
    windows = math.ceil(nodes / _Trie.WALK_LOOKAHEAD)
    assert counting.calls <= (levels + 1) * windows
    assert counting.calls < nodes / 20  # one per node with children before


def test_items_reads_records_in_wide_windows():
    blobs, root, keys = _store()
    nodes, levels = _shape(blobs, root)
    counting = Counting(blobs)
    counting.max_concurrent_reads = 32
    got = list(RecordStore.at(root, counting).items())
    assert [k for k, _ in got] == keys and got[5][1] == {"k": keys[5]}
    record_rounds = math.ceil(len(keys) / 256)  # windows of 256, not of 32
    walk_rounds = (levels + 1) * math.ceil(nodes / _Trie.WALK_LOOKAHEAD)
    assert counting.calls <= walk_rounds + record_rounds


def test_a_tiny_node_cache_still_walks_correctly():
    blobs, root, keys = _store(n=800)
    store = RecordStore(Counting(blobs), root=root, node_cache_size=8)
    assert list(store.keys()) == keys
    assert dict(store.items())[keys[-1]] == {"k": keys[-1]}


def test_a_prefix_scan_loads_only_its_subtree():
    blobs, root, keys = _store()
    prefix = keys[len(keys) // 2].split("/")[0] + "/"
    under = [k for k in keys if k.startswith(prefix)]
    nodes, levels = _shape(blobs, root)
    counting = Counting(blobs)
    assert list(RecordStore.at(root, counting).keys(prefix)) == under
    # the subtree and the path to it, plus siblings checked on the way
    # down — never the whole store
    assert counting.fetched < 4 * len(under) + 40 * levels
    assert counting.fetched < nodes / 4


def _diverged(blobs, root, keys, changes, seed):
    """A root that changes `changes` records of `root` and adds a tenth as
    many, spread across the key space."""
    rng = random.Random(seed)
    store = RecordStore(blobs, root=root)
    for k in rng.sample(keys, changes):
        store.put(k, {"changed": seed})
    for i in range(changes // 10):
        store.put(f"new{seed}/{i}", i)
    return store.commit()


def test_a_diff_costs_rounds_per_level_not_per_node():
    blobs, root, keys = _store(n=5000, seed=3)
    _, levels = _shape(blobs, root)
    other = _diverged(blobs, root, keys, 200, seed=1)
    counting = Counting(blobs)
    changed = list(RecordStore.at(root, counting).diff(other))
    assert len(changed) == 220
    # two tries walked together, plus one window of values
    assert counting.calls <= 2 * (levels + 1) + 1
    assert counting.fetched > 50 * counting.calls  # was one blob per round


def test_the_lookahead_fetches_only_what_the_diff_visits(monkeypatch):
    blobs, root, keys = _store(n=2000, seed=4)
    other = _diverged(blobs, root, keys, 100, seed=2)
    ahead = Counting(blobs)
    got = sorted(RecordStore.at(root, ahead).diff(other))
    monkeypatch.setattr(_Trie, "WALK_LOOKAHEAD", 1)  # one node at a time
    one = Counting(blobs)
    assert sorted(RecordStore.at(root, one).diff(other)) == got
    assert ahead.fetched == one.fetched
    assert ahead.calls < one.calls / 20


def test_a_merge_reads_both_sides_in_few_rounds():
    blobs, root, keys = _store(n=5000, seed=3)
    _, levels = _shape(blobs, root)
    ours = _diverged(blobs, root, keys, 500, seed=1)
    theirs = _diverged(blobs, root, keys, 500, seed=2)
    counting = Counting(blobs)
    merged = RecordStore.merge(counting, root, ours, theirs,
                               resolver=lambda k, b, o, t: "both")
    assert counting.calls <= 4 * (levels + 1) + 10  # two diffs, conflicts
    view = RecordStore.at(merged, blobs)
    assert view.get("new1/0") == 0 and view.get("new2/0") == 0


def test_a_diff_with_a_tiny_node_cache_is_still_exact():
    blobs, root, keys = _store(n=800, seed=6)
    other = _diverged(blobs, root, keys, 60, seed=3)
    full = sorted(RecordStore.at(root, blobs).diff(other))
    tiny = RecordStore(Counting(blobs), root=root, node_cache_size=8)
    assert sorted(tiny.diff(other)) == full
