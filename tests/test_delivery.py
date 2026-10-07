"""An accepted upload is not a delivered one (BeeBytesStore.confirm).

Bee answers an upload once the blob is on the uploading node; delivery to
the neighbourhood that keeps it can fail unnoticed (a "shallow receipt",
seen on Gnosis mainnet in 2026-09 and on 2026-10-07). Here a fake Bee loses
chosen blobs on their first, deferred push: confirm() must find them
through /stewardship, push only them again (directly, from the remembered
bytes, or by asking the node when the bytes were dropped for the budget),
and a RecordStore that opted in must not move its pointer before that.
"""

import hashlib
import types
import unittest

try:
    import requests  # noqa: F401 — BeeBytesStore's own lazy dependency
    HAVE_REQUESTS = True
except ImportError:
    HAVE_REQUESTS = False

from recordstore import MemoryPointer, RecordStore


class FakeBee:
    """/bytes, /stewardship: what the network holds, and what it lost."""

    def __init__(self, lose=(), lose_all=False):
        self.blobs, self.network = {}, set()
        self.lose = set(lose)          # data whose deferred push is lost
        self.lose_all = lose_all       # ... or every deferred push
        self.direct, self.stewarded = [], []

    def _ref(self, data):
        return hashlib.sha256(data).hexdigest()

    def post(self, url, data=None, headers=None, timeout=None):
        ref = self._ref(data)
        self.blobs[ref] = data
        if headers.get("Swarm-Deferred-Upload") == "false":
            self.direct.append(ref)
            self.network.add(ref)
        elif not self.lose_all and data not in self.lose:
            self.network.add(ref)
        return types.SimpleNamespace(status_code=201, text="",
                                     raise_for_status=lambda: None,
                                     json=lambda: {"reference": ref})

    def get(self, url, timeout=None):
        ref = url.rsplit("/", 1)[1]
        if "/stewardship/" in url:
            ok = ref in self.network
            return types.SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                                         json=lambda: {"isRetrievable": ok})
        return types.SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                                     content=self.blobs[ref])

    def put(self, url, headers=None, timeout=None):
        ref = url.rsplit("/", 1)[1]
        self.stewarded.append(ref)
        self.network.add(ref)
        return types.SimpleNamespace(status_code=200, raise_for_status=lambda: None)


@unittest.skipUnless(HAVE_REQUESTS, "requests not installed")
class TestConfirmAndRepair(unittest.TestCase):

    def _store(self, bee, **kw):
        from recordstore.recordstore import BeeBytesStore
        kw.setdefault("repair_after", 0.0)
        store = BeeBytesStore("http://x:1633", "ab" * 32, **kw)
        store._session = bee
        return store

    def test_only_what_the_network_lost_is_pushed_again(self):
        bee = FakeBee(lose={b"lost-1", b"lost-2"})
        store = self._store(bee)
        refs = [store.put(d) for d in (b"fine-1", b"lost-1", b"fine-2", b"lost-2")]
        self.assertEqual(len(store.unconfirmed()), 4)
        store.confirm(timeout=5)
        self.assertEqual(store.unconfirmed(), [])
        self.assertEqual(sorted(bee.direct), sorted([refs[1], refs[3]]))
        self.assertEqual(store.repaired, {refs[1]: 1, refs[3]: 1})

    def test_a_fresh_upload_gets_time_to_spread_before_a_repair(self):
        bee = FakeBee(lose={b"slow"})
        store = self._store(bee, repair_after=3600)
        store.put(b"slow")
        with self.assertRaises(TimeoutError) as ctx:
            store.confirm(timeout=0.2)
        self.assertIn("1 of the blobs", str(ctx.exception))
        self.assertEqual(bee.direct, [])            # not yet due
        self.assertEqual(len(store.unconfirmed()), 1)

    def test_past_the_budget_the_node_is_asked_to_re_push(self):
        bee = FakeBee(lose={b"x" * 100})
        store = self._store(bee, keep_unconfirmed_bytes=50)
        ref = store.put(b"x" * 100)                 # bytes dropped at once
        store.confirm(timeout=5)
        self.assertEqual(bee.stewarded, [ref])
        self.assertEqual(bee.direct, [])

    def test_the_pointer_waits_for_delivery(self):
        """A store that opted in confirms before its pointer moves, so a
        feed never names a root the network cannot serve; without the
        opt-in nothing waits (confirm stays available)."""
        bee = FakeBee(lose_all=True)
        blobs = self._store(bee, confirm_on_commit=True, repair_after=3600,
                            confirm_timeout=0.2)
        pointer = MemoryPointer()
        rs = RecordStore(blobs, pointer=pointer)
        rs.put("k", {"v": 1})
        before = pointer.get()
        with self.assertRaises(TimeoutError):
            rs.commit()
        self.assertEqual(pointer.get(), before)     # not published
        blobs.repair_after = 0.0
        root = rs.commit()                          # the next commit carries on
        self.assertEqual(pointer.get(), root)
        self.assertEqual(blobs.unconfirmed(), [])

    def test_without_the_opt_in_commit_does_not_wait(self):
        bee = FakeBee(lose_all=True)
        blobs = self._store(bee, repair_after=3600)
        pointer = MemoryPointer()
        rs = RecordStore(blobs, pointer=pointer)
        rs.put("k", {"v": 1})
        root = rs.commit()
        self.assertEqual(pointer.get(), root)
        self.assertTrue(blobs.unconfirmed())


if __name__ == "__main__":
    unittest.main()
