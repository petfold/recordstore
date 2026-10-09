"""Integration test: SwarmFeedPointer against a live Bee node.

Requires a running Bee node and swarmfs with coincurve (the `feeds` extra):

    pip install "recordstore[feeds]"
    bee dev --api-addr=127.0.0.1:1633
    BEE_API=http://127.0.0.1:1633 python3 -m pytest tests/test_recordstore_feed.py -v

A random signer key and a postage batch are created automatically unless
BEE_FEED_SIGNER / BEE_BATCH are set. Skipped entirely when BEE_API is unset or
the `feeds` extra is not installed, so it is safe in CI.

Feed lookups are unreliable per call on Swarm (see the SwarmFeedPointer
docstring); these tests exercise the read-your-writes cache and the
retry-until-stable read path that exist precisely to paper over that.
"""

import importlib
import os
import secrets
import time
import unittest

BEE_API = os.environ.get("BEE_API")

try:
    importlib.import_module("coincurve")          # feed signing
    importlib.import_module("swarmfs")
    _HAVE_FEEDS = True
except ImportError:
    _HAVE_FEEDS = False


@unittest.skipUnless(BEE_API, "set BEE_API to run Bee integration tests")
@unittest.skipUnless(_HAVE_FEEDS, "install recordstore[feeds] (swarmfs + coincurve)")
class TestSwarmFeedPointer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import json
        import urllib.request

        cls.batch = os.environ.get("BEE_BATCH")
        if not cls.batch:
            req = urllib.request.Request(f"{BEE_API}/stamps/100000000/20", method="POST")
            with urllib.request.urlopen(req, timeout=60) as r:
                cls.batch = json.load(r)["batchID"]
            deadline = time.time() + 120
            while time.time() < deadline:  # wait until the batch is usable
                try:
                    with urllib.request.urlopen(f"{BEE_API}/stamps/{cls.batch}", timeout=30) as s:
                        if json.load(s).get("usable"):
                            break
                except OSError:
                    pass
                time.sleep(2)
            else:
                raise RuntimeError("postage batch never became usable")
        cls.signer = os.environ.get("BEE_FEED_SIGNER") or secrets.token_hex(32)

    def _pointer(self, topic, **kw):
        from recordstore import SwarmFeedPointer
        return SwarmFeedPointer(
            BEE_API, topic, signer=self.signer, postage_batch_id=self.batch, **kw
        )

    def _unique_topic(self, label):
        # A fresh topic per test run keeps feed indices independent.
        return f"recordstore/test/{label}/{os.getpid()}/{secrets.token_hex(4)}"

    def test_read_your_writes_serves_from_cache(self):
        p = self._pointer(self._unique_topic("ryw"))
        ref = secrets.token_hex(32)
        p.set(ref)
        # Served from the local cache within feed_ttl — no flaky lookup involved.
        self.assertEqual(p.get(), ref)

    def test_fresh_reader_resolves_over_network(self):
        topic = self._unique_topic("net")
        ref = secrets.token_hex(32)
        self._pointer(topic).set(ref)
        # A brand-new instance has an empty cache, so this goes to the network
        # and exercises the retry-until-stable loop.
        reader = self._pointer(topic)
        self.assertEqual(reader.get(), ref)

    def test_read_only_pointer_by_owner(self):
        topic = self._unique_topic("readonly")
        writer = self._pointer(topic)
        ref = secrets.token_hex(32)
        writer.set(ref)

        from recordstore import SwarmFeedPointer
        owner = writer.owner  # address derived from the signer
        reader = SwarmFeedPointer(BEE_API, topic, owner=owner)
        self.assertEqual(reader.get(), ref)
        with self.assertRaises(RuntimeError):
            reader.set(secrets.token_hex(32))  # no signer => cannot write

    def test_latest_of_two_writes(self):
        topic = self._unique_topic("seq")
        p = self._pointer(topic)
        p.set(secrets.token_hex(32))
        second = secrets.token_hex(32)
        p.set(second)  # index floor prevents reusing the first index
        self.assertEqual(self._pointer(topic).get(), second)

    def test_after_hint_resolves_latest(self):
        # feed_ttl=0 disables the read-your-writes cache, forcing get() onto the
        # network path; after three writes the index floor is high enough that
        # get() resolves via Bee's `after` hint rather than a plain lookup.
        topic = self._unique_topic("afterhint")
        p = self._pointer(topic, feed_ttl=0.0)
        refs = [secrets.token_hex(32) for _ in range(3)]
        for r in refs:
            p.set(r)
        self.assertEqual(p.get(), refs[-1])

    def test_cold_read_resolves_without_retries(self):
        # Cold reads resolve via reliable SOC probing, not the flaky /feeds
        # lookup, so a single attempt (no retries) suffices.
        topic = self._unique_topic("coldprobe")
        ref = secrets.token_hex(32)
        self._pointer(topic).set(ref)
        reader = self._pointer(topic, max_lookup_retries=1)
        self.assertEqual(reader.get(), ref)

    def test_empty_feed_reads_as_none(self):
        # Never-written feed: retries exhaust and get() reports None (empty),
        # which RecordStore treats as "start from the empty dataset".
        reader = self._pointer(self._unique_topic("empty"), max_lookup_retries=2,
                               retry_backoff=0.1)
        self.assertIsNone(reader.get())

    def test_compare_and_set(self):
        topic = self._unique_topic("cas")
        a, b = secrets.token_hex(32), secrets.token_hex(32)
        self.assertTrue(self._pointer(topic).compare_and_set(None, a))  # empty feed
        self.assertEqual(self._pointer(topic).get(), a)
        # stale expected (feed holds `a`, not None) -> refused
        self.assertFalse(self._pointer(topic).compare_and_set(None, b))
        # correct expected -> accepted
        self.assertTrue(self._pointer(topic).compare_and_set(a, b))
        self.assertEqual(self._pointer(topic).get(), b)

    def test_reconcile_over_feed(self):
        from recordstore import BeeBytesStore, RecordStore
        topic = self._unique_topic("recon")
        blobs = BeeBytesStore(BEE_API, self.batch)
        seed = RecordStore(blobs, pointer=self._pointer(topic))
        seed.put("a", 1)
        seed.commit()
        w1 = RecordStore(blobs, pointer=self._pointer(topic))  # both branch off seed
        w2 = RecordStore(blobs, pointer=self._pointer(topic))
        w1.put("b", 2)
        w2.put("c", 3)
        w1.commit(reconcile=True)
        w2.commit(reconcile=True)  # sees w1's advance -> merges instead of clobbering
        final = RecordStore(blobs, pointer=self._pointer(topic))
        self.assertEqual(dict(final.items()), {"a": 1, "b": 2, "c": 3})

    def test_the_signed_sequence_of_roots_verifies_offline(self):
        # 2026-09-29: the feed's updates, read back from the node as raw
        # owner-signed chunks, verify with no node: the published order of
        # a store's roots, for a third party.
        from recordstore import SwarmFeedPointer, verify_feed_update
        topic = self._unique_topic("seq")
        writer = self._pointer(topic)
        roots = [secrets.token_hex(32) for _ in range(3)]
        for r in roots:
            writer.set(r)
        owner = writer.owner
        reader = SwarmFeedPointer(BEE_API, topic, owner=owner)
        envelopes = None
        for _ in range(10):                       # a fresh chunk may take a moment to be retrievable
            try:
                envelopes = reader.updates(0, 3)
                break
            except Exception:  # noqa: BLE001
                time.sleep(2)
        self.assertIsNotNone(envelopes)
        updates = [verify_feed_update(e, owner, topic) for e in envelopes]
        self.assertEqual([(u.index, u.root) for u in updates], list(enumerate(roots)))
        now = int(time.time())
        self.assertTrue(all(abs(u.timestamp - now) < 3600 for u in updates))   # the writer's own clock
        from recordstore import ProofError
        with self.assertRaises(ProofError):
            verify_feed_update(envelopes[0], "0x" + "33" * 20, topic)

    def test_end_to_end_recordstore_over_feed(self):
        from recordstore import BeeBytesStore, RecordStore

        topic = self._unique_topic("e2e")
        blobs = BeeBytesStore(BEE_API, self.batch)
        rs = RecordStore(blobs, pointer=self._pointer(topic))
        rs.put("users/alice", {"name": "Alice"})
        rs.commit()  # advances the feed pointer to the new root

        # Reopen from just the feed pointer (no root passed): resolves the
        # latest root off the feed and reads the record back.
        reopened = RecordStore(blobs, pointer=self._pointer(topic))
        self.assertEqual(reopened.get("users/alice"), {"name": "Alice"})


if __name__ == "__main__":
    unittest.main()
