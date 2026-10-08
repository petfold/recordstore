"""The feed pointer's chunk probe retries a transient 500 (0.20.3): an absent
chunk answers 404 definitively, a struggling node answers 500 for the same
chunk and must be asked again — `set` probes cold and had no retry of its
own, so one flaky read failed a fresh feed's first commit. Needs only
swarmfs (the feed reads are stubbed; no node)."""

import unittest
from types import SimpleNamespace

try:
    from swarmfs.exceptions import BeeAPIError
    _HAVE_SWARMFS = True
except ImportError:
    _HAVE_SWARMFS = False


@unittest.skipUnless(_HAVE_SWARMFS, "install recordstore[bee] (swarmfs)")
class TestProbeRetries(unittest.TestCase):
    def _pointer(self, answers, retries=4):
        from recordstore import SwarmFeedPointer
        p = SwarmFeedPointer("http://127.0.0.1:1", "t", owner="0x" + "11" * 20,
                             max_lookup_retries=retries, retry_backoff=0, retry_backoff_cap=0)
        calls = []

        def at_index(owner, topic, index, verify=False):
            calls.append(index)
            status = answers[min(len(calls), len(answers)) - 1]
            if status == 200:
                return SimpleNamespace(reference="11" * 32, index=index)
            if status == 404:
                raise FileNotFoundError(index)  # what swarmfs raises on a 404
            raise BeeAPIError(status, "chunks")

        p._run = lambda fn, *a, **kw: fn(*a, **kw)
        p._ops = SimpleNamespace(at_index=at_index)
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
        with self.assertRaises(BeeAPIError):
            p._probe_latest_index()
        self.assertEqual(len(calls), 3)

    def test_a_404_is_never_retried(self):
        p, calls = self._pointer([404])
        self.assertIsNone(p._probe_latest_index())
        self.assertEqual(len(calls), 1)
