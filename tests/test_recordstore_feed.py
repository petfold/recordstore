"""Integration test: SwarmFeedPointer against a live Bee node + swarm-bee.

Requires both a running Bee node and the `swarm-bee` package:

    pip install "recordstore[feeds]"
    bee dev --api-addr=127.0.0.1:1633
    BEE_API=http://127.0.0.1:1633 python3 -m pytest tests/test_recordstore_feed.py -v

A random signer key and a postage batch are created automatically unless
BEE_FEED_SIGNER / BEE_BATCH are set. Skipped entirely when BEE_API is unset or
`swarm-bee` is not installed, so it is safe in CI.

Feed lookups are unreliable per call on Swarm (see the SwarmFeedPointer
docstring); these tests exercise the read-your-writes cache and the
retry-until-stable read path that exist precisely to paper over that.
"""

import os
import secrets
import time
import unittest

BEE_API = os.environ.get("BEE_API")

try:
    import bee as _bee  # noqa: F401  (import-name of the `swarm-bee` package)
    _HAVE_SWARM_BEE = True
except ImportError:
    _HAVE_SWARM_BEE = False


@unittest.skipUnless(BEE_API, "set BEE_API to run Bee integration tests")
@unittest.skipUnless(_HAVE_SWARM_BEE, "install recordstore[feeds] (swarm-bee)")
class TestSwarmFeedPointer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import requests

        cls.batch = os.environ.get("BEE_BATCH")
        if not cls.batch:
            r = requests.post(f"{BEE_API}/stamps/100000000/20", timeout=60)
            r.raise_for_status()
            cls.batch = r.json()["batchID"]
            deadline = time.time() + 120
            while time.time() < deadline:  # wait until the batch is usable
                s = requests.get(f"{BEE_API}/stamps/{cls.batch}", timeout=30)
                if s.ok and s.json().get("usable"):
                    break
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
        owner = writer._owner.to_hex()  # address derived from the signer
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

    def test_empty_feed_reads_as_none(self):
        # Never-written feed: retries exhaust and get() reports None (empty),
        # which RecordStore treats as "start from the empty dataset".
        reader = self._pointer(self._unique_topic("empty"), max_lookup_retries=2,
                               retry_backoff=0.1)
        self.assertIsNone(reader.get())

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
