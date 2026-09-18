"""The feed pointer's chunk probe retries a transient 500 (0.20.3): an absent
chunk answers 404 definitively, a struggling node answers 500 for the same
chunk and must be asked again — `set` probes cold and had no retry of its
own, so one flaky read failed a fresh feed's first commit. Needs only the
`swarm-bee` import (the client object is stubbed; no node)."""

import unittest
from types import SimpleNamespace

try:
    import bee as _bee  # noqa: F401
    _HAVE_SWARM_BEE = True
except ImportError:
    _HAVE_SWARM_BEE = False


class _Refused(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


@unittest.skipUnless(_HAVE_SWARM_BEE, "install recordstore[feeds] (swarm-bee)")
class TestProbeRetries(unittest.TestCase):
    def _pointer(self, answers, retries=4):
        from recordstore import SwarmFeedPointer
        p = SwarmFeedPointer("http://127.0.0.1:1", "t", owner="0x" + "11" * 20,
                             max_lookup_retries=retries, retry_backoff=0, retry_backoff_cap=0)
        calls = []

        def download_soc(owner, identifier):
            calls.append(identifier)
            status = answers[min(len(calls), len(answers)) - 1]
            if status == 200:
                return SimpleNamespace(payload=b"\0" * 8 + b"\x11" * 32)
            raise _Refused(status)

        p._BeeResponseError = _Refused
        p._bee = SimpleNamespace(file=SimpleNamespace(download_soc=download_soc))
        return p, calls

    def test_a_500_is_asked_again_and_a_404_is_the_answer(self):
        p, calls = self._pointer([500, 500, 404])
        self.assertIsNone(p._probe_latest_index())      # an empty feed, after two retries
        self.assertEqual(len(calls), 3)

    def test_a_500_is_asked_again_and_a_chunk_is_found(self):
        p, calls = self._pointer([500, 200, 404])
        # index 0 exists (after one retry); index 1 does not: the tip is 0
        p2, calls2 = self._pointer([500, 200, 404, 404])
        self.assertEqual(p2._probe_latest_index(), 0)

    def test_retries_spent_raises_the_last_500(self):
        p, calls = self._pointer([500], retries=3)
        with self.assertRaises(_Refused):
            p._probe_latest_index()
        self.assertEqual(len(calls), 3)

    def test_a_404_is_never_retried(self):
        p, calls = self._pointer([404])
        self.assertIsNone(p._probe_latest_index())
        self.assertEqual(len(calls), 1)
