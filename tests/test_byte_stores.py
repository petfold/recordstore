"""DirBytesStore and FsspecBytesStore: durable blobs without Swarm.

Both are BytesStore implementations, so the contract under test is the one
RecordStore relies on: put returns a reference derived from the content, get
round-trips, a missing reference raises KeyError, and identical content is
stored once. The end-to-end assertion is that a full RecordStore works over
them and that roots match MemoryBytesStore's — which is what "portable
between backends" means for sha256 addressing.
"""

import os
import shutil
import tempfile
import unittest

from recordstore import (DirBytesStore, FsspecBytesStore, MemoryBytesStore,
                         RecordStore)

try:
    import fsspec  # noqa: F401
    HAVE_FSSPEC = True
except ImportError:
    HAVE_FSSPEC = False


class BytesStoreContract:
    """Shared expectations; subclasses provide `make_store`."""

    def test_put_get_roundtrip(self):
        s = self.make_store()
        ref = s.put(b"hello blobs")
        self.assertEqual(s.get(ref), b"hello blobs")

    def test_reference_is_derived_from_content(self):
        a, b = self.make_store(), self.make_store()
        self.assertEqual(a.put(b"same"), b.put(b"same"))
        self.assertNotEqual(a.put(b"one"), a.put(b"two"))

    def test_identical_content_stored_once(self):
        s = self.make_store()
        first = s.put(b"dup")
        again = s.put(b"dup")          # must not error, must not duplicate
        self.assertEqual(first, again)
        self.assertEqual(s.get(first), b"dup")

    def test_missing_reference_raises_keyerror(self):
        s = self.make_store()
        with self.assertRaises(KeyError):
            s.get("00" * 32)

    def test_empty_and_large_values(self):
        s = self.make_store()
        for data in (b"", b"x" * 1_000_000):
            self.assertEqual(s.get(s.put(data)), data)

    def test_get_many(self):
        s = self.make_store()
        refs = [s.put(f"v{i}".encode()) for i in range(5)]
        got = s.get_many(refs)
        self.assertEqual({r: got[r] for r in refs},
                         {r: f"v{i}".encode() for i, r in enumerate(refs)})

    def test_full_recordstore_over_it(self):
        s = self.make_store()
        store = RecordStore(s)
        store.put("alpha", {"n": 1})
        store.put("beta", {"n": 2})
        root = store.commit()
        reopened = RecordStore.at(root, s)
        self.assertEqual(sorted(reopened.keys()), ["alpha", "beta"])
        self.assertEqual(reopened.get("alpha"), {"n": 1})

    def test_root_matches_memory_backend(self):
        """sha256 addressing means a dataset has the same root in memory and
        on disk — the property that makes backends interchangeable."""
        def build(bytes_store):
            st = RecordStore(bytes_store)
            st.put("a", {"x": [1, 2, 3]})
            st.put("b", "text")
            return st.commit()

        self.assertEqual(build(self.make_store()), build(MemoryBytesStore()))


class TestDirBytesStore(BytesStoreContract, unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self._n = 0

    def make_store(self):
        self._n += 1
        return DirBytesStore(os.path.join(self.tmp, f"blobs{self._n}"))

    def test_survives_process_restart(self):
        """The point of this class: a new instance over the same directory sees
        everything the old one wrote."""
        path = os.path.join(self.tmp, "durable")
        first = DirBytesStore(path)
        store = RecordStore(first)
        store.put("kept", {"across": "restarts"})
        root = store.commit()

        reopened = RecordStore.at(root, DirBytesStore(path))   # fresh instance
        self.assertEqual(reopened.get("kept"), {"across": "restarts"})

    def test_fanned_out_layout(self):
        path = os.path.join(self.tmp, "fanout")
        s = DirBytesStore(path)
        ref = s.put(b"payload")
        self.assertTrue(os.path.exists(os.path.join(path, ref[:2], ref[2:])))

    def test_no_temp_files_left_behind(self):
        path = os.path.join(self.tmp, "clean")
        s = DirBytesStore(path)
        for i in range(20):
            s.put(f"v{i}".encode())
        leftovers = [f for _, _, files in os.walk(path) for f in files if ".tmp" in f]
        self.assertEqual(leftovers, [])

    def test_expanduser(self):
        s = DirBytesStore(os.path.join(self.tmp, "x"))
        self.assertTrue(os.path.isabs(s.path))

    def test_rejects_unknown_addressing(self):
        with self.assertRaises(ValueError) as ctx:
            DirBytesStore(os.path.join(self.tmp, "bad"), addressing="md5")
        self.assertIn("unknown addressing", str(ctx.exception))

    def test_custom_addressing_callable(self):
        import hashlib
        s = DirBytesStore(os.path.join(self.tmp, "custom"),
                          addressing=lambda d: hashlib.sha1(d).hexdigest())
        ref = s.put(b"abc")
        self.assertEqual(ref, hashlib.sha1(b"abc").hexdigest())
        self.assertEqual(s.get(ref), b"abc")


@unittest.skipUnless(HAVE_FSSPEC, "needs fsspec")
class TestFsspecBytesStore(BytesStoreContract, unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self._n = 0

    def make_store(self):
        self._n += 1
        return FsspecBytesStore(os.path.join(self.tmp, f"fs{self._n}"))

    def test_memory_filesystem(self):
        s = FsspecBytesStore("memory://blobs-mem")
        ref = s.put(b"in memory via fsspec")
        self.assertEqual(s.get(ref), b"in memory via fsspec")

    def test_refuses_swarm_protocols(self):
        """Swarm is content-addressed: the reference comes *out* of the write,
        so a path-addressed store would discard it."""
        for url in ("bzz://something/blobs", "bzzf://owner/topic"):
            with self.assertRaises(ValueError) as ctx:
                FsspecBytesStore(url)
            msg = str(ctx.exception)
            self.assertIn("path-addressed", msg)
            self.assertIn("BeeBytesStore", msg)


class TestSwarmAddressing(unittest.TestCase):
    """Optional addressing that makes a local directory share Swarm's address
    space. Skipped unless swarmfs (with keccak) is importable."""

    def setUp(self):
        try:
            from swarmfs.splitter import content_address  # noqa: F401
        except ImportError:
            self.skipTest("needs swarmfs[feeds]")
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def test_reference_is_the_swarm_reference(self):
        from swarmfs.splitter import content_address

        s = DirBytesStore(os.path.join(self.tmp, "swarm"), addressing="swarm")
        for data in (b"", b"tiny", b"y" * 10_000):
            self.assertEqual(s.put(data), content_address(data).hex())
            self.assertEqual(s.get(content_address(data).hex()), data)

    def test_differs_from_sha256_addressing(self):
        sha = DirBytesStore(os.path.join(self.tmp, "sha"))
        swarm = DirBytesStore(os.path.join(self.tmp, "sw"), addressing="swarm")
        self.assertNotEqual(sha.put(b"same bytes"), swarm.put(b"same bytes"))


if __name__ == "__main__":
    unittest.main()
