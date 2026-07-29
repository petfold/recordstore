"""BeeBytesStore's 'auto' postage-batch selection and batch health
(offline; swarmfs is faked). Selection and inspection only — buying and
renewing are deliberately the caller's move, since a library must not
spend the node wallet's xBZZ."""

import sys
import types
import unittest
import warnings
from unittest import mock

from recordstore.recordstore import (
    AUTO_MIN_BATCH_TTL,
    _auto_batch,
    batch_status,
)


def _info(**kw):
    """A stand-in for swarmfs's StampInfo, healthy unless overridden."""
    base = dict(batch_id="ef" * 32, usable=True, ttl=40 * 86400,
                utilization_ratio=0.5, label="", immutable=True, depth=19,
                amount=1, bucket_depth=16, utilization=4, bucket_capacity=8)
    return types.SimpleNamespace(**{**base, **kw})


def _fake_swarmfs(info=None, buckets=None, legacy=False, record=None):
    """Fake swarmfs modules with the 0.4.0 stamp surface.

    ``legacy=True`` omits .buckets, standing in for swarmfs < 0.4.0.
    """
    client_mod = types.ModuleType("swarmfs._client")
    stamps_mod = types.ModuleType("swarmfs.stamps")

    class FakeClient:
        def __init__(self, api_url):
            self.api_url = api_url

        async def close(self):
            pass

    class FakeManager:
        def __init__(self, client, min_ttl=60):
            self.client = client
            if record is not None:
                record["min_ttl"] = min_ttl

        async def resolve(self, stamp):
            assert stamp == "auto"
            return (info or _info()).batch_id

        async def get_batch(self, batch_id):
            return info or _info()

    if not legacy:  # .buckets arrived in swarmfs 0.4.0; its presence is the probe
        async def _buckets(self, batch_id):
            return buckets

        FakeManager.buckets = _buckets

    client_mod.SwarmClient = FakeClient
    stamps_mod.StampManager = FakeManager
    return {
        "swarmfs": types.ModuleType("swarmfs"),
        "swarmfs._client": client_mod,
        "swarmfs.stamps": stamps_mod,
    }


try:
    import requests  # noqa: F401 — BeeBytesStore's own lazy dependency
    HAVE_REQUESTS = True
except ImportError:  # pragma: no cover
    HAVE_REQUESTS = False


class TestAutoStamp(unittest.TestCase):
    @unittest.skipUnless(HAVE_REQUESTS, "requests not installed")
    def test_explicit_batch_id_bypasses_selection(self):
        from recordstore.recordstore import BeeBytesStore

        with mock.patch(
            "recordstore.recordstore._auto_batch",
            side_effect=AssertionError("must not be called"),
        ):
            store = BeeBytesStore("http://x:1633", "ab" * 32)
        self.assertEqual(store.batch, "ab" * 32)

    @unittest.skipUnless(HAVE_REQUESTS, "requests not installed")
    def test_auto_resolves_via_helper(self):
        from recordstore.recordstore import BeeBytesStore

        with mock.patch(
            "recordstore.recordstore._auto_batch", return_value="cd" * 32
        ) as m:
            store = BeeBytesStore("http://x:1633")  # default is 'auto'
        m.assert_called_once_with("http://x:1633", AUTO_MIN_BATCH_TTL)
        self.assertEqual(store.batch, "cd" * 32)

        # the floor is a constructor knob: a short-lived store can opt down
        with mock.patch(
            "recordstore.recordstore._auto_batch", return_value="cd" * 32
        ) as m:
            BeeBytesStore("http://x:1633", min_batch_ttl=3600)
        m.assert_called_once_with("http://x:1633", 3600)

    def test_auto_batch_bridges_to_swarmfs(self):
        record = {}
        with mock.patch.dict(sys.modules, _fake_swarmfs(record=record)):
            with warnings.catch_warnings():
                warnings.simplefilter("error")  # a healthy batch must be quiet
                self.assertEqual(_auto_batch("http://x:1633"), "ef" * 32)
        # a record store outlives the one-shot upload swarmfs's 60 s floor is
        # written for, so it asks for a day
        self.assertEqual(record["min_ttl"], AUTO_MIN_BATCH_TTL)
        self.assertEqual(AUTO_MIN_BATCH_TTL, 86400)

        with mock.patch.dict(sys.modules, _fake_swarmfs(record=record)):
            _auto_batch("http://x:1633", 3600)
        self.assertEqual(record["min_ttl"], 3600)  # caller can override

    def test_expiring_batch_warns_with_the_cure(self):
        expiring = _info(ttl=3 * 86400)
        with mock.patch.dict(sys.modules, _fake_swarmfs(expiring)):
            with self.assertWarns(UserWarning) as ctx:
                _auto_batch("http://x:1633", 60)
        msg = str(ctx.warning)
        self.assertIn("3.0 days", msg)
        self.assertIn("cannot be revived", msg)
        self.assertIn("topup", msg)  # names the fix, not just the problem

    def test_nearly_full_immutable_batch_warns_to_dilute(self):
        full = _info(utilization=7, utilization_ratio=0.875)
        with mock.patch.dict(sys.modules, _fake_swarmfs(full)):
            with self.assertWarns(UserWarning) as ctx:
                _auto_batch("http://x:1633")
        msg = str(ctx.warning)
        self.assertIn("88%", msg)
        self.assertIn("overissued", msg)
        self.assertIn("does not lose what is", msg)  # the reassuring truth
        self.assertIn("dilute", msg)

        # a mutable batch has no bucket-overflow refusal, so no warning
        mutable = _info(utilization=7, utilization_ratio=0.875, immutable=False)
        with mock.patch.dict(sys.modules, _fake_swarmfs(mutable)):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                _auto_batch("http://x:1633")

    def test_batch_status_is_read_only_and_optionally_exact(self):
        stats = types.SimpleNamespace(max_load=4, capacity=8, chunks=16279)
        with mock.patch.dict(sys.modules, _fake_swarmfs(buckets=stats)):
            info, buckets = batch_status("http://x:1633", "ef" * 32)
            self.assertEqual(info.batch_id, "ef" * 32)
            self.assertIsNone(buckets)  # the 2 MB histogram is opt-in
            info, buckets = batch_status("http://x:1633", "ef" * 32, buckets=True)
            self.assertEqual(buckets.max_load, 4)

    def test_old_swarmfs_is_actionable_about_the_version(self):
        with mock.patch.dict(sys.modules, _fake_swarmfs(legacy=True)):
            with self.assertRaises(ImportError) as ctx:
                _auto_batch("http://x:1633")
        msg = str(ctx.exception)
        self.assertIn("0.4.0", msg)
        self.assertIn("buckets", msg)

    def test_missing_swarmfs_is_actionable(self):
        with mock.patch.dict(sys.modules, {
            "swarmfs": None, "swarmfs._client": None, "swarmfs.stamps": None,
        }):
            with self.assertRaises(ImportError) as ctx:
                _auto_batch("http://x:1633")
        msg = str(ctx.exception)
        self.assertIn("swarmfs", msg)
        self.assertIn("explicit batch id", msg)



class TestWriteRefusal(unittest.TestCase):
    """A 402 on write means one of two very different things, and only one
    of them is recoverable. BeeBytesStore owns its transport, so it must
    say which itself — it does not inherit swarmfs's 402 handling."""

    def _store(self, status, text):
        from recordstore.recordstore import BeeBytesStore

        store = BeeBytesStore("http://x:1633", "ab" * 32)
        resp = types.SimpleNamespace(status_code=status, text=text)
        store._session = types.SimpleNamespace(post=lambda *a, **k: resp)
        return store

    @unittest.skipUnless(HAVE_REQUESTS, "requests not installed")
    def test_full_bucket_says_dilute_and_that_nothing_is_lost(self):
        store = self._store(402, '{"code":402,"message":"batch is overissued"}')
        with self.assertRaises(RuntimeError) as ctx:
            store.put(b"x")
        msg = str(ctx.exception)
        self.assertIn("a bucket is full", msg)
        self.assertIn("Nothing already stored is lost", msg)
        self.assertIn("Dilute", msg)
        self.assertIn("top up", msg)

    @unittest.skipUnless(HAVE_REQUESTS, "requests not installed")
    def test_other_402_points_at_the_batch_and_expiry(self):
        store = self._store(402, '{"code":402,"message":"batch not usable"}')
        with self.assertRaises(RuntimeError) as ctx:
            store.put(b"x")
        msg = str(ctx.exception)
        self.assertIn("did not accept postage batch", msg)
        self.assertIn("cannot be revived", msg)
        self.assertNotIn("bucket is full", msg)

if __name__ == "__main__":
    unittest.main()
