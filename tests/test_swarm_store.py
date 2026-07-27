"""The swarm_store factory: the single place Swarm is specified.

Offline — BeeBytesStore and SwarmFeedPointer are faked, so this checks the
wiring (which backend, which pointer, shared postage batch) rather than
Bee behaviour, which the live tests cover.
"""

import sys
import types
import unittest
from unittest import mock

from recordstore import swarm_store
import recordstore.recordstore as rs


class FakeBlobs:
    def __init__(self, api_url, batch, **kw):
        self.api_url, self.batch, self.kw = api_url, batch, kw


class FakePointer:
    def __init__(self, api_url, topic, **kw):
        self.api_url, self.topic, self.kw = api_url, topic, kw

    def get(self):
        return None


class TestSwarmStore(unittest.TestCase):
    def _patched(self):
        return mock.patch.multiple(rs, BeeBytesStore=FakeBlobs,
                                   SwarmFeedPointer=FakePointer)

    def test_needs_signer_or_owner(self):
        with self.assertRaises(ValueError) as ctx:
            swarm_store("topic")
        self.assertIn("signer", str(ctx.exception))
        self.assertIn("owner", str(ctx.exception))

    def test_wires_bee_blobs_and_a_feed_pointer(self):
        with self._patched():
            store = swarm_store("my-notes", signer="ab" * 32,
                                api_url="http://node:1633/")
        self.assertIsInstance(store._blobs, FakeBlobs)
        self.assertIsInstance(store._pointer, FakePointer)
        self.assertEqual(store._pointer.topic, "my-notes")
        self.assertEqual(store._pointer.kw["signer"], "ab" * 32)

    def test_blobs_and_feed_share_one_resolved_batch(self):
        """The batch is resolved once by the blob store (possibly from
        "auto") and handed to the feed, so SOC writes and blob writes are
        paid from the same batch."""
        with self._patched():
            store = swarm_store("t", signer="cd" * 32, stamp="ef" * 32)
        self.assertEqual(store._blobs.batch, "ef" * 32)
        self.assertEqual(store._pointer.kw["postage_batch_id"], "ef" * 32)

    def test_read_only_view_by_owner(self):
        with self._patched():
            store = swarm_store("t", owner="12" * 20)
        self.assertEqual(store._pointer.kw["owner"], "12" * 20)
        self.assertIsNone(store._pointer.kw["signer"])

    def test_passes_through_transport_options(self):
        with self._patched():
            store = swarm_store("t", signer="ab" * 32, feed_ttl=99.0,
                                max_concurrent_reads=4, deferred_upload=False)
        self.assertEqual(store._pointer.kw["feed_ttl"], 99.0)
        self.assertEqual(store._blobs.kw["max_concurrent_reads"], 4)
        self.assertFalse(store._blobs.kw["deferred_upload"])


if __name__ == "__main__":
    unittest.main()
