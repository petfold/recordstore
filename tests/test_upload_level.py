"""Every upload names its redundancy level (ontodag's question 47, decided
2026-10-15): 1 for a blob that fits one chunk, 0 above, so a reference is
always the one Swarm's own rules give (swarmfs.splitter.content_address),
never one only the node's coding could tell. Left to Bee's default (1), a
blob over 4 KB came back coded under another reference."""

import asyncio
import importlib.util
import os
import types
import unittest

HAVE_SWARMFS = importlib.util.find_spec("swarmfs") is not None
BEE_API = os.environ.get("BEE_API")


@unittest.skipUnless(HAVE_SWARMFS, "swarmfs not installed")
class TestBeeBytesStoreNamesTheLevel(unittest.TestCase):
    SIZES = (100, 4096, 4097, 10_000)
    LEVELS = [(100, 1), (4096, 1), (4097, 0), (10_000, 0)]

    def test_put(self):
        from recordstore.recordstore import BeeBytesStore
        sent = []
        store = BeeBytesStore("http://x:1633", "ab" * 32)
        store._client = types.SimpleNamespace(bytes_post=lambda data, batch, deferred=None, redundancy=None: (
            sent.append((len(data), redundancy)) or "00" * 32))
        for n in self.SIZES:
            store.put(b"x" * n)
        self.assertEqual(sent, self.LEVELS)

    def test_put_many(self):
        from recordstore.recordstore import BeeBytesStore
        sent = []

        async def post(data, batch, deferred=None, redundancy=None):
            await asyncio.sleep(0)
            sent.append((len(data), redundancy))
            return "00" * 32
        store = BeeBytesStore("http://x:1633", "ab" * 32)
        store._async = types.SimpleNamespace(bytes_post=post)
        store.put_many([b"x" * n for n in self.SIZES])
        self.assertEqual(sorted(sent), self.LEVELS)


@unittest.skipUnless(BEE_API and os.environ.get("BEE_BATCH") and HAVE_SWARMFS,
                     "set BEE_API and BEE_BATCH to run against a live node")
class TestAgainstLiveBee(unittest.TestCase):
    def test_references_are_the_ones_computed_locally(self):
        from swarmfs.splitter import content_address

        from recordstore.recordstore import BeeBytesStore
        store = BeeBytesStore(BEE_API, os.environ["BEE_BATCH"])
        for data in (os.urandom(100), os.urandom(10_000)):
            self.assertEqual(store.put(data), content_address(data).hex())
        datas = [os.urandom(200), os.urandom(9_000)]
        self.assertEqual(store.put_many(datas), [content_address(d).hex() for d in datas])
