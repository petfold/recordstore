"""Tests for recordstore.

The properties tested here are the ones the OntoDAG-on-Swarm design
depends on:

  R1  Roundtrip & staging      - put/get/delete with read-your-writes
  R2  Canonical roots          - same content => same root, regardless of
                                 insertion order or history (CRDT precondition)
  R3  Snapshot isolation       - a reader pinned to a root is unaffected by
                                 later commits; commits are all-or-nothing
  R4  Structural sharing       - a small change writes O(depth) blobs,
                                 not O(dataset)
  R5  Prefix iteration         - sorted, namespace-style key listing,
                                 staged overlay included
  R6  No aliasing              - mutating a returned record never mutates
                                 the store
  R7  Pointer persistence      - FilePointer survives process restart and
                                 is updated atomically on commit
"""

import os
import random
import tempfile
import unittest

from recordstore.recordstore import _Trie, _decode_value

from recordstore import (ABSENT, DELETE, FilePointer, MemoryBytesStore,
                         MergeConflict, MemoryPointer, RecordStore)


def make(bytes_store=None, pointer=None):
    return RecordStore(bytes_store or MemoryBytesStore(), pointer=pointer)


class TestRoundtrip(unittest.TestCase):
    def test_put_get_commit_get(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        rec = {"up": ["vehicle"], "down": [], "count": 0}
        rs.put("car", rec)
        self.assertEqual(rs.get("car"), rec)          # read-your-writes
        root = rs.commit()
        self.assertIsNotNone(root)
        again = RecordStore.at(root, store)
        self.assertEqual(again.get("car"), rec)

    def test_delete(self):
        rs = make()
        rs.put("a", 1)
        rs.put("ab", 2)
        rs.commit()
        rs.delete("a")
        self.assertFalse(rs.contains("a"))
        self.assertTrue(rs.contains("ab"))
        rs.commit()
        self.assertFalse(rs.contains("a"))
        with self.assertRaises(KeyError):
            rs.get("a")
        with self.assertRaises(KeyError):
            rs.delete("never-existed")

    def test_empty_and_missing(self):
        rs = make()
        with self.assertRaises(KeyError):
            rs.get("nothing")
        with self.assertRaises(ValueError):
            rs.put("", 1)
        self.assertIsNone(rs.commit())               # empty commit, empty store


class TestCanonicalRoots(unittest.TestCase):
    RECORDS = {f"ns:key{i:03d}": {"n": i, "tags": ["a", "b"]} for i in range(60)}

    def _root_via(self, order, extra_churn=False):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        for k in order:
            rs.put(k, self.RECORDS[k])
            rs.commit()                              # one commit per key
        if extra_churn:                              # divergent history
            rs.put("ephemeral", {"x": 1})
            rs.commit()
            rs.delete("ephemeral")
            rs.commit()
        return rs.root

    def test_order_and_history_independent_root(self):
        keys = list(self.RECORDS)
        forward = self._root_via(keys)
        backward = self._root_via(list(reversed(keys)))
        churned = self._root_via(sorted(keys, key=hash), extra_churn=True)
        self.assertEqual(forward, backward)
        self.assertEqual(forward, churned)

    def test_batched_vs_incremental_same_root(self):
        keys = list(self.RECORDS)
        incremental = self._root_via(keys)
        store = MemoryBytesStore()
        rs = RecordStore(store)
        for k in keys:
            rs.put(k, self.RECORDS[k])
        batched = rs.commit()                        # single commit
        self.assertEqual(incremental, batched)


class TestSnapshotIsolation(unittest.TestCase):
    def test_reader_pinned_to_old_root(self):
        store = MemoryBytesStore()
        writer = RecordStore(store)
        writer.put("car", {"count": 1})
        root1 = writer.commit()
        reader = RecordStore.at(root1, store)

        writer.put("car", {"count": 2})
        writer.put("bike", {"count": 0})
        writer.commit()

        self.assertEqual(reader.get("car"), {"count": 1})
        self.assertFalse(reader.contains("bike"))
        self.assertEqual(list(reader.keys()), ["car"])

    def test_snapshot_is_readonly(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        rs.put("a", 1)
        snap = RecordStore.at(rs.commit(), store)
        with self.assertRaises(TypeError):
            snap.put("b", 2)
        with self.assertRaises(TypeError):
            snap.commit()

    def test_pointer_moves_only_on_commit(self):
        ptr = MemoryPointer()
        rs = make(pointer=ptr)
        rs.put("a", 1)
        self.assertIsNone(ptr.get())                 # staged, not visible
        root = rs.commit()
        self.assertEqual(ptr.get(), root)


class TestStructuralSharing(unittest.TestCase):
    def test_small_update_writes_few_blobs(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        for i in range(200):
            rs.put(f"node{i:04d}", {"payload": i})
        rs.commit()
        before = len(store)
        rs.put("node0042", {"payload": "changed"})
        rs.commit()
        added = len(store) - before
        # one value blob + the rewritten trie path; must be far below 200
        self.assertLess(added, 12, f"update rewrote {added} blobs")


class TestPrefixIteration(unittest.TestCase):
    def test_sorted_namespace_listing_with_overlay(self):
        rs = make()
        for k in ["veh:car", "veh:bike", "food:apple", "veh:boat"]:
            rs.put(k, {})
        rs.commit()
        rs.put("veh:van", {})                        # staged add
        rs.delete("veh:bike")                        # staged delete
        self.assertEqual(list(rs.keys("veh:")),
                         ["veh:boat", "veh:car", "veh:van"])
        self.assertEqual(list(rs.keys("food:")), ["food:apple"])
        self.assertEqual(list(rs.keys("nope:")), [])


class TestFilePointer(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._dir.name, "root")

    def tearDown(self):
        self._dir.cleanup()

    def test_missing_file_reads_as_none(self):
        self.assertIsNone(FilePointer(self.path).get())

    def test_set_get_roundtrip(self):
        ptr = FilePointer(self.path)
        ptr.set("abc123")
        self.assertEqual(ptr.get(), "abc123")
        self.assertEqual(FilePointer(self.path).get(), "abc123")  # fresh instance

    def test_commit_persists_root_across_restart(self):
        store = MemoryBytesStore()
        rs = RecordStore(store, pointer=FilePointer(self.path))
        rs.put("car", {"up": ["vehicle"]})
        root = rs.commit()
        self.assertEqual(FilePointer(self.path).get(), root)
        # a fresh store + fresh pointer resumes at the committed root
        again = RecordStore(store, pointer=FilePointer(self.path))
        self.assertEqual(again.get("car"), {"up": ["vehicle"]})

    def test_second_commit_replaces_root_without_tmp_residue(self):
        store = MemoryBytesStore()
        rs = RecordStore(store, pointer=FilePointer(self.path))
        rs.put("a", 1)
        first = rs.commit()
        rs.put("b", 2)
        second = rs.commit()
        self.assertNotEqual(first, second)
        self.assertEqual(FilePointer(self.path).get(), second)
        self.assertFalse(os.path.exists(self.path + ".tmp"))


class TestNoAliasing(unittest.TestCase):
    def test_returned_records_are_detached(self):
        rs = make()
        original = {"up": ["a"], "meta": {"kind": "class"}}
        rs.put("x", original)
        original["up"].append("HACK")                # mutate after put
        self.assertEqual(rs.get("x")["up"], ["a"])
        fetched = rs.get("x")
        fetched["meta"]["kind"] = "instance"         # mutate a returned copy
        self.assertEqual(rs.get("x")["meta"]["kind"], "class")
        rs.commit()
        self.assertEqual(rs.get("x")["up"], ["a"])


class _CountingStore(MemoryBytesStore):
    """MemoryBytesStore that counts serial gets vs batched get_many calls.
    get_many reads the dict directly so it does not inflate the get counter."""

    def __init__(self):
        super().__init__()
        self.get_calls = 0
        self.get_many_calls = 0

    def get(self, ref):
        self.get_calls += 1
        return super().get(ref)

    def get_many(self, refs):
        self.get_many_calls += 1
        return {ref: self.blobs[ref] for ref in refs}


class TestBulkItems(unittest.TestCase):
    def _populate(self, store, n=40):
        rs = RecordStore(store)
        for i in range(n):
            rs.put(f"k{i:03d}", {"n": i})
        return rs.commit()

    def test_items_matches_keys_and_get(self):
        store = MemoryBytesStore()
        root = self._populate(store)
        rs = RecordStore.at(root, store)
        expected = {k: rs.get(k) for k in rs.keys()}
        self.assertEqual(dict(rs.items()), expected)
        self.assertEqual([k for k, _ in rs.items()], sorted(expected))  # sorted

    def test_items_uses_batched_reads_only(self):
        store = _CountingStore()
        root = self._populate(store, 40)
        rs = RecordStore.at(root, store)  # fresh trie cache => real fetches
        store.get_calls = store.get_many_calls = 0
        result = dict(rs.items())
        self.assertEqual(len(result), 40)
        self.assertGreaterEqual(store.get_many_calls, 1)  # batch path taken
        self.assertEqual(store.get_calls, 0)              # no serial per-blob gets

    def test_items_staged_overlay_and_tombstone(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        rs.put("a", 1)
        rs.put("b", 2)
        rs.commit()
        rs.put("c", 3)
        rs.delete("a")
        self.assertEqual(dict(rs.items()), {"b": 2, "c": 3})

    def test_items_deep_copied(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        rs.put("x", {"list": [1, 2]})
        rs.commit()
        again = RecordStore.at(rs.root, store)
        dict(again.items())["x"]["list"].append(999)  # mutate a returned copy
        self.assertEqual(again.get("x"), {"list": [1, 2]})  # store unmutated

    def test_items_empty(self):
        self.assertEqual(list(RecordStore(MemoryBytesStore()).items()), [])

    def test_items_windowed_multiple_flushes(self):
        # small window forces several batched flushes; output must stay complete
        # and globally sorted across window boundaries.
        class Small(MemoryBytesStore):
            max_concurrent_reads = 3

        store = Small()
        rs = RecordStore(store)
        expected = {f"k{i:02d}": {"n": i} for i in range(10)}
        for k, v in expected.items():
            rs.put(k, v)
        root = rs.commit()
        got = list(RecordStore.at(root, store).items())
        self.assertEqual(dict(got), expected)
        self.assertEqual([k for k, _ in got], sorted(expected))

    def test_keys_lazy_generator(self):
        rs = make()
        for k in ("b", "a", "c"):
            rs.put(k, k)
        rs.commit()
        gen = rs.keys()
        self.assertEqual(next(gen), "a")  # partial consumption, not materialized


class TestBulkCommit(unittest.TestCase):
    def _counting(self):
        class C(MemoryBytesStore):
            def __init__(s):
                super().__init__()
                s.puts = s.put_many_calls = s.put_many_items = 0

            def put(s, d):
                s.puts += 1
                return super().put(d)

            def put_many(s, ds):
                ds = list(ds)
                s.put_many_calls += 1
                s.put_many_items += len(ds)
                return [MemoryBytesStore.put(s, d) for d in ds]  # bypass put counter
        return C()

    def test_commit_writes_are_batched_and_pruned(self):
        store = self._counting()
        rs = RecordStore(store)
        recs = {f"k{i:03d}": {"n": i} for i in range(20)}
        for k, v in recs.items():
            rs.put(k, v)
        root = rs.commit()
        # all writes went through the batched path; none serial
        self.assertEqual(store.puts, 0)
        # O(depth) batches (1 value level + a few trie levels), not O(nodes)
        self.assertLessEqual(store.put_many_calls, 8)
        # orphaned intermediate nodes were never written
        self.assertLess(store.put_many_items, 20 * 3)
        # every key survives the reachability prune
        self.assertEqual(dict(RecordStore.at(root, store).items()), recs)

    def test_bulk_commit_matches_incremental_root(self):
        # one big commit vs many small ones => identical canonical root
        a = RecordStore(MemoryBytesStore())
        for i in range(30):
            a.put(f"x/{i:02d}", {"n": i})
        root_bulk = a.commit()

        b = RecordStore(MemoryBytesStore())
        for i in range(30):
            b.put(f"x/{i:02d}", {"n": i})
            b.commit()
        self.assertEqual(root_bulk, b.root)

    def test_bulk_delete(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        for i in range(10):
            rs.put(f"k{i}", i)
        rs.commit()
        for i in range(0, 10, 2):
            rs.delete(f"k{i}")
        root = rs.commit()
        self.assertEqual(dict(RecordStore.at(root, store).items()),
                         {f"k{i}": i for i in range(1, 10, 2)})

    def test_mixed_put_delete_in_one_commit(self):
        store = MemoryBytesStore()
        rs = RecordStore(store)
        rs.put("a", 1)
        rs.put("b", 2)
        rs.commit()
        rs.put("c", 3)
        rs.delete("a")
        rs.put("b", 22)
        root = rs.commit()
        self.assertEqual(dict(RecordStore.at(root, store).items()),
                         {"b": 22, "c": 3})


class TestMerge(unittest.TestCase):
    def setUp(self):
        self.store = MemoryBytesStore()

    def _root(self, mapping):
        rs = RecordStore(self.store)
        for k, v in mapping.items():
            rs.put(k, v)
        return rs.commit()

    def _dict(self, root):
        return dict(RecordStore.at(root, self.store).items())

    def test_disjoint_changes_merge(self):
        base = self._root({"a": 1, "b": 2, "c": 3})
        ours = self._root({"a": 10, "b": 2, "c": 3})
        theirs = self._root({"a": 1, "b": 20, "c": 3})
        m = RecordStore.merge(self.store, base, ours, theirs)
        self.assertEqual(self._dict(m), {"a": 10, "b": 20, "c": 3})

    def test_same_change_on_both_sides(self):
        base = self._root({"a": 1})
        side = self._root({"a": 2})
        m = RecordStore.merge(self.store, base, side, self._root({"a": 2}))
        self.assertEqual(self._dict(m), {"a": 2})
        self.assertEqual(m, side)

    def test_fast_paths(self):
        base = self._root({"a": 1})
        ours = self._root({"a": 2})
        self.assertEqual(RecordStore.merge(self.store, base, ours, base), ours)
        self.assertEqual(RecordStore.merge(self.store, base, base, ours), ours)
        self.assertEqual(RecordStore.merge(self.store, base, ours, ours), ours)

    def test_conflict_raises_by_default(self):
        base = self._root({"a": 1})
        with self.assertRaises(MergeConflict) as cm:
            RecordStore.merge(self.store, base,
                              self._root({"a": 2}), self._root({"a": 3}))
        self.assertEqual(cm.exception.conflicts, ["a"])

    def test_conflict_resolved(self):
        base = self._root({"a": 1})
        m = RecordStore.merge(self.store, base,
                              self._root({"a": 2}), self._root({"a": 3}),
                              resolver=lambda k, b, o, t: max(o, t))
        self.assertEqual(self._dict(m), {"a": 3})

    def test_add_add_conflict(self):
        base = self._root({})
        ours = self._root({"new": "ours"})
        theirs = self._root({"new": "theirs"})
        with self.assertRaises(MergeConflict):
            RecordStore.merge(self.store, base, ours, theirs)
        m = RecordStore.merge(self.store, base, ours, theirs,
                              resolver=lambda k, b, o, t: o)
        self.assertEqual(self._dict(m), {"new": "ours"})

    def test_delete_on_one_side(self):
        base = self._root({"a": 1, "b": 2})
        ours = self._root({"a": 1})              # deleted b
        theirs = base
        m = RecordStore.merge(self.store, base, ours, theirs)
        self.assertEqual(self._dict(m), {"a": 1})

    def test_delete_vs_modify_conflict(self):
        base = self._root({"a": 1})
        ours = self._root({})                    # deleted a
        theirs = self._root({"a": 2})            # modified a
        with self.assertRaises(MergeConflict):
            RecordStore.merge(self.store, base, ours, theirs)
        seen = {}

        def r(k, b, o, t):
            seen.update(base=b, ours=o, theirs=t)
            return t

        m = RecordStore.merge(self.store, base, ours, theirs, resolver=r)
        self.assertEqual(seen, {"base": 1, "ours": ABSENT, "theirs": 2})
        self.assertEqual(self._dict(m), {"a": 2})

    def test_resolver_delete_sentinel(self):
        base = self._root({"a": 1})
        m = RecordStore.merge(self.store, base,
                              self._root({"a": 2}), self._root({"a": 3}),
                              resolver=lambda k, b, o, t: DELETE)
        self.assertEqual(self._dict(m), {})

    def test_merge_is_canonical_and_commutative(self):
        base = self._root({"a": 1, "b": 2, "c": 3})
        ours = self._root({"a": 10, "b": 2, "c": 3})
        theirs = self._root({"a": 1, "b": 20, "c": 3})
        m1 = RecordStore.merge(self.store, base, ours, theirs)
        m2 = RecordStore.merge(self.store, base, theirs, ours)
        direct = self._root({"a": 10, "b": 20, "c": 3})
        self.assertEqual(m1, m2)       # commutative (canonical roots)
        self.assertEqual(m1, direct)   # == a direct build of the merged content

    def test_merge_no_common_ancestor(self):
        m = RecordStore.merge(self.store, None,
                              self._root({"a": 1}), self._root({"b": 2}))
        self.assertEqual(self._dict(m), {"a": 1, "b": 2})


class TestReconcilingCommit(unittest.TestCase):
    def _seed(self, mapping):
        self.store = MemoryBytesStore()
        self.ptr = MemoryPointer()
        seed = RecordStore(self.store, pointer=self.ptr)
        for k, v in mapping.items():
            seed.put(k, v)
        seed.commit()

    def _final(self):
        return dict(RecordStore(self.store, pointer=self.ptr).items())

    def test_concurrent_writers_converge(self):
        self._seed({"a": 1, "b": 2})
        w1 = RecordStore(self.store, pointer=self.ptr)  # both open at the base
        w2 = RecordStore(self.store, pointer=self.ptr)
        w1.put("a", 10)
        w2.put("b", 20)
        w1.commit(reconcile=True)   # pointer unchanged -> lands directly
        w2.commit(reconcile=True)   # pointer moved -> merges w1's change in
        self.assertEqual(self._final(), {"a": 10, "b": 20})

    def test_reconcile_conflict_raises(self):
        self._seed({"a": 1})
        w1 = RecordStore(self.store, pointer=self.ptr)
        w2 = RecordStore(self.store, pointer=self.ptr)
        w1.put("a", 2)
        w2.put("a", 3)
        w1.commit(reconcile=True)
        with self.assertRaises(MergeConflict):
            w2.commit(reconcile=True)

    def test_reconcile_conflict_resolved(self):
        self._seed({"a": 1})
        w1 = RecordStore(self.store, pointer=self.ptr)
        w2 = RecordStore(self.store, pointer=self.ptr)
        w1.put("a", 2)
        w2.put("a", 3)
        w1.commit(reconcile=True)
        w2.commit(reconcile=True, resolver=lambda k, b, o, t: max(o, t))
        self.assertEqual(self._final(), {"a": 3})

    def test_default_commit_is_last_write_wins(self):
        self._seed({"a": 1, "b": 2})
        w1 = RecordStore(self.store, pointer=self.ptr)
        w2 = RecordStore(self.store, pointer=self.ptr)
        w1.put("a", 10)
        w2.put("b", 20)
        w1.commit()   # no reconcile
        w2.commit()   # overwrites: w1's change to a is lost from latest
        self.assertEqual(self._final(), {"a": 1, "b": 20})

    def test_many_writers_converge(self):
        # N>2 writers converge by cascading pairwise merges through the pointer.
        self._seed({"base": 0})
        writers = [RecordStore(self.store, pointer=self.ptr) for _ in range(5)]
        for i, w in enumerate(writers):
            w.put(f"w{i}", i)          # disjoint keys
        for w in writers:
            w.commit(reconcile=True)   # each folds in whatever landed before it
        self.assertEqual(self._final(),
                         {"base": 0, "w0": 0, "w1": 1, "w2": 2, "w3": 3, "w4": 4})

    def test_many_writers_conflict_with_commutative_resolver(self):
        # same key, three writers; an associative+commutative resolver (max)
        # makes the outcome independent of commit order.
        self._seed({"k": 0})
        vals = [5, 9, 3]
        writers = [RecordStore(self.store, pointer=self.ptr) for _ in vals]
        for w, v in zip(writers, vals):
            w.put("k", v)
        for w in writers:
            w.commit(reconcile=True, resolver=lambda key, b, o, t: max(o, t))
        self.assertEqual(self._final(), {"k": 9})

    def test_reconcile_single_writer_unaffected(self):
        self._seed({"a": 1})
        w = RecordStore(self.store, pointer=self.ptr)
        w.put("b", 2)
        root = w.commit(reconcile=True)
        self.assertEqual(self.ptr.get(), root)
        self.assertEqual(self._final(), {"a": 1, "b": 2})


class TestDiffMergeFuzz(unittest.TestCase):
    """The radix diff powering merge is intricate (prefix splits); check it and
    the whole merge against brute-force oracles over many random cases with
    shared-prefix keys."""

    def _build(self, store, mapping):
        rs = RecordStore(store)
        for k, v in mapping.items():
            rs.put(k, v)
        return rs.commit()

    def _rand_map(self, rng, n):
        return {"".join(rng.choice("abc") for _ in range(rng.randint(1, 5))):
                rng.randint(0, 9) for _ in range(n)}

    def test_diff_matches_bruteforce(self):
        rng = random.Random(20260720)
        for _ in range(400):
            store = MemoryBytesStore()
            am = self._rand_map(rng, rng.randint(0, 15))
            bm = self._rand_map(rng, rng.randint(0, 15))
            a, b = self._build(store, am), self._build(store, bm)
            trie = _Trie(store)
            got = {}
            for k, av, bv in trie._diff(a, b):
                got[k.decode()] = (
                    _decode_value(store.get(av)) if av is not None else None,
                    _decode_value(store.get(bv)) if bv is not None else None)
            expected = {k: (am.get(k), bm.get(k))
                        for k in set(am) | set(bm) if am.get(k) != bm.get(k)}
            self.assertEqual(got, expected)

    def test_merge_matches_bruteforce(self):
        rng = random.Random(11111)

        def mutate(base):
            m = dict(base)
            for _ in range(rng.randint(0, 6)):
                k = "".join(rng.choice("abc") for _ in range(rng.randint(1, 4)))
                if rng.random() < 0.7:
                    m[k] = rng.randint(0, 99)
                else:
                    m.pop(k, None)
            return m

        def resolver(k, b, o, t):  # symmetric, deterministic; never DELETE
            return max(x for x in (o, t) if x is not ABSENT)

        def oracle(base, ours, theirs):
            out = {}
            for k in set(base) | set(ours) | set(theirs):
                b = base.get(k, ABSENT)
                o = ours.get(k, ABSENT)
                t = theirs.get(k, ABSENT)
                if o == t:
                    v = o
                elif o == b:
                    v = t
                elif t == b:
                    v = o
                else:
                    v = resolver(k, b, o, t)
                if v is not ABSENT:
                    out[k] = v
            return out

        for _ in range(400):
            store = MemoryBytesStore()
            base = self._rand_map(rng, rng.randint(0, 12))
            ours, theirs = mutate(base), mutate(base)
            m = RecordStore.merge(store, self._build(store, base),
                                  self._build(store, ours),
                                  self._build(store, theirs), resolver=resolver)
            self.assertEqual(dict(RecordStore.at(m, store).items()),
                             oracle(base, ours, theirs))


if __name__ == "__main__":
    unittest.main()
