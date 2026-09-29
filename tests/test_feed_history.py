"""Feed updates as a verifiable sequence of roots (SwarmFeedPointer.update /
updates, verify_feed_update, verify_equivocation).

  F1  An update envelope carries the raw owner-signed chunk; verified with no
      node, it returns (index, root, the writer's timestamp).
  F2  Another owner, another feed, another index, a tampered chunk or
      envelope — each raises ProofError.
  F3  Two different roots signed at one index prove equivocation; two
      updates at different indices, or the same root twice, do not.
  F4  The pointer reads one update, or the sequence up to the tip, as
      envelopes (the node is stubbed).

Chunks are built with swarm-bee's own signing — the code the pointer
writes with, verified against a live Bee — so they are exactly what a feed
publishes. Needs the `swarm-bee` import only."""

import json
import unittest
from types import SimpleNamespace

try:
    import bee as _bee  # noqa: F401
    _HAVE_SWARM_BEE = True
except ImportError:
    _HAVE_SWARM_BEE = False

KEY = "11" * 32
OTHER_KEY = "22" * 32
TOPIC = "loopmarket-register-demo"
ROOT_A, ROOT_B = "aa" * 32, "bb" * 32


def _signed(index, root, *, key=KEY, topic=TOPIC, timestamp=1_790_000_000):
    """The feed update chunk `set` would publish, as an envelope."""
    from bee.feeds import make_feed_identifier
    from bee.swarm.keys import PrivateKey
    from bee.swarm.soc import make_single_owner_chunk
    from bee.swarm.typed_bytes import Topic
    from recordstore import FEED_UPDATE_FORMAT
    signer = PrivateKey.from_hex(key)
    t = Topic.from_string(topic)
    soc = make_single_owner_chunk(make_feed_identifier(t, index),
                                  timestamp.to_bytes(8, "big") + bytes.fromhex(root), signer)
    return {"format": FEED_UPDATE_FORMAT, "version": 1, "owner": signer.public_key().address().as_bytes().hex(),
            "topic": t.as_bytes().hex(), "index": index, "soc": soc.data().hex()}


def _owner(key=KEY):
    from bee.swarm.keys import PrivateKey
    return "0x" + PrivateKey.from_hex(key).public_key().address().as_bytes().hex()


@unittest.skipUnless(_HAVE_SWARM_BEE, "install recordstore[feeds] (swarm-bee)")
class TestVerify(unittest.TestCase):
    def test_an_update_verifies_offline_to_its_index_root_and_timestamp(self):
        from recordstore import FeedUpdate, verify_feed_update
        u = _signed(3, ROOT_A, timestamp=1234)
        self.assertEqual(verify_feed_update(u, _owner(), TOPIC), FeedUpdate(3, ROOT_A, 1234))
        # the topic may be given as its 32-byte hex, the owner without 0x; the envelope travels
        self.assertEqual(verify_feed_update(json.loads(json.dumps(u)), _owner()[2:], u["topic"]).root, ROOT_A)

    def test_any_mismatch_is_refused(self):
        from recordstore import ProofError, verify_feed_update
        u = _signed(3, ROOT_A)
        bad = [
            (dict(u), _owner(OTHER_KEY), TOPIC),                     # another owner
            (dict(u), _owner(), "another-feed"),                     # another feed
            (dict(u, index=4), _owner(), TOPIC),                     # another index
            (dict(u, soc=u["soc"][:-2] + ("00" if u["soc"][-2:] != "00" else "01")), _owner(), TOPIC),
            (dict(u, format="recordstore-trie-proof"), _owner(), TOPIC),
            (dict(u, version=2), _owner(), TOPIC),
            (_signed(3, ROOT_A, key=OTHER_KEY), _owner(), TOPIC),    # signed by someone else
        ]
        for envelope, owner, topic in bad:
            with self.assertRaises(ProofError):
                verify_feed_update(envelope, owner, topic)

    def test_two_roots_at_one_index_prove_equivocation(self):
        from recordstore import ProofError, verify_equivocation
        self.assertEqual(verify_equivocation(_signed(5, ROOT_A), _signed(5, ROOT_B), _owner(), TOPIC), 5)
        with self.assertRaises(ProofError):                          # a sequence, not a conflict
            verify_equivocation(_signed(5, ROOT_A), _signed(6, ROOT_B), _owner(), TOPIC)
        with self.assertRaises(ProofError):                          # the same root re-signed
            verify_equivocation(_signed(5, ROOT_A, timestamp=1), _signed(5, ROOT_A, timestamp=2),
                                _owner(), TOPIC)
        with self.assertRaises(ProofError):                          # one of them another owner's
            verify_equivocation(_signed(5, ROOT_A), _signed(5, ROOT_B, key=OTHER_KEY), _owner(), TOPIC)


@unittest.skipUnless(_HAVE_SWARM_BEE, "install recordstore[feeds] (swarm-bee)")
class TestPointer(unittest.TestCase):
    def _pointer(self, roots):
        from bee.swarm.soc import calculate_single_owner_chunk_address
        from recordstore import SwarmFeedPointer
        p = SwarmFeedPointer("http://127.0.0.1:1", TOPIC, owner=_owner())
        chunks = {}
        for i, root in enumerate(roots):
            env = _signed(i, root)
            address = calculate_single_owner_chunk_address(p._make_feed_identifier(p._topic, i), p._owner)
            chunks[address.as_bytes()] = bytes.fromhex(env["soc"])
        p._bee = SimpleNamespace(file=SimpleNamespace(download_chunk=lambda a: chunks[a.as_bytes()]))
        p._probe_latest_index = lambda: len(roots) - 1 if roots else None
        return p

    def test_the_pointer_reads_the_signed_sequence_of_roots(self):
        from recordstore import verify_feed_update
        p = self._pointer([ROOT_A, ROOT_B, "cc" * 32])
        u = p.update(1)
        self.assertEqual(verify_feed_update(u, _owner(), TOPIC).root, ROOT_B)
        seq = [verify_feed_update(x, _owner(), TOPIC) for x in p.updates()]
        self.assertEqual([(s.index, s.root) for s in seq], [(0, ROOT_A), (1, ROOT_B), (2, "cc" * 32)])
        self.assertEqual(len(p.updates(1, 2)), 1)
        self.assertEqual(self._pointer([]).updates(), [])


if __name__ == "__main__":
    unittest.main()
