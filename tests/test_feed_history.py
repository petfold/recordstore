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

Chunks are built with swarmfs's signer — the code the pointer writes
with since 0.22, verified against a live Bee — so they are exactly what a
feed publishes; one test checks they are byte-identical to the chunks
swarm-bee (the pointer's library before 0.22) builds, when it is
installed. Needs swarmfs with coincurve (to sign the fixtures)."""

import json
import unittest
from types import SimpleNamespace

try:
    import coincurve  # noqa: F401  (to sign the fixtures)
    import swarmfs  # noqa: F401
    _HAVE_SIGNER = True
except ImportError:
    _HAVE_SIGNER = False

KEY = "11" * 32
OTHER_KEY = "22" * 32
TOPIC = "loopmarket-register-demo"
ROOT_A, ROOT_B = "aa" * 32, "bb" * 32


def _signed(index, root, *, key=KEY, topic=TOPIC, timestamp=1_790_000_000):
    """The feed update chunk `set` would publish, as an envelope."""
    from recordstore import FEED_UPDATE_FORMAT
    from swarmfs.bmt import cac_data, chunk_address, keccak256
    from swarmfs.feeds import feed_identifier
    from swarmfs.signer import Signer
    signer = Signer(key)
    t = keccak256(topic.encode())
    identifier = feed_identifier(t, index)
    cac = cac_data(timestamp.to_bytes(8, "big") + bytes.fromhex(root))
    soc = identifier + signer.sign(identifier + chunk_address(cac)) + cac
    return {"format": FEED_UPDATE_FORMAT, "version": 1, "owner": signer.address_hex,
            "topic": t.hex(), "index": index, "soc": soc.hex()}


def _owner(key=KEY):
    from swarmfs.signer import Signer
    return "0x" + Signer(key).address_hex


@unittest.skipUnless(_HAVE_SIGNER, "install recordstore[feeds] (swarmfs + coincurve)")
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


@unittest.skipUnless(_HAVE_SIGNER, "install recordstore[feeds] (swarmfs + coincurve)")
class TestPointer(unittest.TestCase):
    def _pointer(self, roots):
        from recordstore import SwarmFeedPointer
        from swarmfs.feeds import feed_identifier, soc_address
        p = SwarmFeedPointer("http://127.0.0.1:1", TOPIC, owner=_owner())
        chunks = {}
        for i, root in enumerate(roots):
            env = _signed(i, root)
            address = soc_address(feed_identifier(bytes.fromhex(p.topic), i), bytes.fromhex(p.owner))
            chunks[address.hex()] = bytes.fromhex(env["soc"])
        p._client = SimpleNamespace(chunk_get=lambda a: chunks[a])
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


class TestSameChunksAsSwarmBee(unittest.TestCase):
    """Feeds published before 0.22 were signed by swarm-bee: the chunks the
    pointer writes now are byte-identical, so old and new envelopes verify
    alike and either reader follows the other's feed."""

    def test_byte_identical_to_swarm_bee(self):
        try:
            from bee.feeds import make_feed_identifier
            from bee.swarm.keys import PrivateKey
            from bee.swarm.soc import make_single_owner_chunk
            from bee.swarm.typed_bytes import Topic
        except ImportError:
            self.skipTest("swarm-bee not installed (only needed for this comparison)")
        if not _HAVE_SIGNER:
            self.skipTest("swarmfs + coincurve needed")
        for index, root, key in ((0, ROOT_A, KEY), (7, ROOT_B, OTHER_KEY)):
            old = make_single_owner_chunk(
                make_feed_identifier(Topic.from_string(TOPIC), index),
                (1_790_000_000).to_bytes(8, "big") + bytes.fromhex(root),
                PrivateKey.from_hex(key)).data()
            self.assertEqual(_signed(index, root, key=key)["soc"], bytes(old).hex())


if __name__ == "__main__":
    unittest.main()
