"""BeeBytesStore's 'auto' postage-batch selection (offline; swarmfs is
faked). Selection only — buying is deliberately the caller's move."""

import sys
import types
import unittest
from unittest import mock

from recordstore.recordstore import _auto_batch

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
        m.assert_called_once_with("http://x:1633")
        self.assertEqual(store.batch, "cd" * 32)

    def test_auto_batch_bridges_to_swarmfs(self):
        fake_client_mod = types.ModuleType("swarmfs._client")
        fake_stamps_mod = types.ModuleType("swarmfs.stamps")

        class FakeClient:
            def __init__(self, api_url):
                self.api_url = api_url

            async def close(self):
                pass

        class FakeManager:
            def __init__(self, client):
                self.client = client

            async def resolve(self, stamp):
                assert stamp == "auto"
                return "ef" * 32

        fake_client_mod.SwarmClient = FakeClient
        fake_stamps_mod.StampManager = FakeManager
        with mock.patch.dict(sys.modules, {
            "swarmfs": types.ModuleType("swarmfs"),
            "swarmfs._client": fake_client_mod,
            "swarmfs.stamps": fake_stamps_mod,
        }):
            self.assertEqual(_auto_batch("http://x:1633"), "ef" * 32)

    def test_missing_swarmfs_is_actionable(self):
        with mock.patch.dict(sys.modules, {
            "swarmfs": None, "swarmfs._client": None, "swarmfs.stamps": None,
        }):
            with self.assertRaises(ImportError) as ctx:
                _auto_batch("http://x:1633")
        msg = str(ctx.exception)
        self.assertIn("swarmfs", msg)
        self.assertIn("explicit batch id", msg)


if __name__ == "__main__":
    unittest.main()
