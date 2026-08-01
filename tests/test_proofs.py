"""Inclusion and absence proofs (RecordStore.prove / verify_proof).

The properties that matter:

  P1  Every committed key has an inclusion proof whose verification
      returns the record, with no store access.
  P2  Every missing key has an ABSENCE proof — the canonical encoding
      gives a key exactly one possible location, in all three divergence
      shapes (prefix mismatch, key exhausted on a valueless node, no
      child to descend into) — and the empty store proves everything out.
  P3  Verification is adversarial: any tampering — nodes, value, claim
      flag, root, truncation, extension, format — raises ProofError.
  P4  Proofs are JSON round-trippable (they travel as text).
  P5  Proofs speak only about committed roots: staged keys are refused.
"""

import json
import random
import unittest

from recordstore import (ABSENT, MemoryBytesStore, ProofError, RecordStore,
                         verify_proof)

KEY_POOL = (
    ["a", "ab", "abc", "abcd", "abcdef", "abd", "b", "ba", "bab"]
    + ["ns:" + s for s in ("x", "xy", "xyz", "xz", "y")]
    + ["común", "comú", "日本語", "日本", "🐝", "🐝🐝"]
    + [f"k{i:02d}" for i in range(20)]
)


def populated():
    rs = RecordStore(MemoryBytesStore())
    content = {
        "a": {"up": ["b"], "count": 1},
        "ab": [1, 2, 3],
        "abc": "deep",
        "abd": None,          # a stored None is a value, not an absence
        "ns:x": {"nested": {"deep": True}},
        "日本語": "unicode",
    }
    for key, value in content.items():
        rs.put(key, value)
    rs.commit()
    return rs, content


class TestInclusion(unittest.TestCase):
    def test_every_key_proves_and_verifies_to_its_record(self):
        rs, content = populated()
        for key, value in content.items():
            proof = rs.prove(key)
            self.assertTrue(proof["present"], key)
            self.assertEqual(verify_proof(proof, rs.root), value, key)

    def test_verification_needs_no_store(self):
        rs, content = populated()
        proof = rs.prove("ns:x")
        root = rs.root
        del rs  # the verifier holds only the envelope and the root
        self.assertEqual(verify_proof(proof, root), content["ns:x"])

    def test_proofs_are_json_round_trippable(self):
        rs, content = populated()
        for key in content:
            wire = json.dumps(rs.prove(key))
            self.assertEqual(verify_proof(json.loads(wire), rs.root),
                             content[key])

    def test_proof_is_short(self):
        # O(depth), not O(dataset): the path for one key among many stays
        # a handful of nodes.
        rs = RecordStore(MemoryBytesStore())
        for i in range(500):
            rs.put(f"k{i:04d}", i)
        rs.commit()
        proof = rs.prove("k0250")
        self.assertLess(len(proof["nodes"]), 12)


class TestAbsence(unittest.TestCase):
    def test_absent_keys_prove_out_in_every_divergence_shape(self):
        rs, _ = populated()
        for key in ("az",        # diverges inside a stored prefix
                    "abcd",      # walk dies with no child to descend into
                    "abcdefg",   # extension past a leaf
                    "zzz",       # foreign from the first byte
                    "日本"):      # proper prefix of a stored key, no value
            proof = rs.prove(key)
            self.assertFalse(proof["present"], key)
            self.assertIsNone(proof["value"], key)
            self.assertIs(verify_proof(proof, rs.root), ABSENT, key)

    def test_the_empty_store_proves_everything_absent(self):
        rs = RecordStore(MemoryBytesStore())
        proof = rs.prove("anything")
        self.assertEqual(proof["nodes"], [])
        self.assertIs(verify_proof(proof, None), ABSENT)

    def test_deleted_key_proves_absent_after_commit(self):
        rs, _ = populated()
        rs.delete("ab")
        rs.commit()
        self.assertIs(verify_proof(rs.prove("ab"), rs.root), ABSENT)


class TestTampering(unittest.TestCase):
    def setUp(self):
        self.rs, self.content = populated()
        self.root = self.rs.root

    def expect_error(self, proof, root=None, fragment=""):
        with self.assertRaises(ProofError) as ctx:
            verify_proof(proof, self.root if root is None else root)
        if fragment:
            self.assertIn(fragment, str(ctx.exception))

    def test_flipped_node_byte(self):
        proof = self.rs.prove("abc")
        blob = bytearray(bytes.fromhex(proof["nodes"][0]))
        blob[0] ^= 0xFF
        proof["nodes"][0] = bytes(blob).hex()
        self.expect_error(proof, fragment="does not hash")

    def test_swapped_value(self):
        proof = self.rs.prove("abc")
        other = self.rs.prove("ab")
        proof["value"] = other["value"]
        self.expect_error(proof, fragment="value bytes")

    def test_flipped_claim(self):
        proof = self.rs.prove("abc")
        proof["present"] = False
        proof["value"] = None
        self.expect_error(proof, fragment="contradicts")
        absent = self.rs.prove("zzz")
        absent["present"] = True
        self.expect_error(absent, fragment="contradicts")

    def test_wrong_root(self):
        proof = self.rs.prove("abc")
        self.expect_error(proof, root="00" * 32, fragment="about root")

    def test_truncated_and_extended_paths(self):
        proof = self.rs.prove("abc")
        truncated = dict(proof, nodes=proof["nodes"][:-1])
        self.expect_error(truncated, fragment="ends before")
        extended = dict(proof, nodes=proof["nodes"] + [proof["nodes"][-1]])
        self.expect_error(extended, fragment="past the walk")

    def test_wrong_format_and_version(self):
        proof = self.rs.prove("abc")
        self.expect_error(dict(proof, format="not-a-proof"),
                          fragment="envelope")
        self.expect_error(dict(proof, version=99), fragment="version")

    def test_key_substitution_fails(self):
        # A proof for one key cannot be replayed as a proof about another.
        proof = self.rs.prove("abc")
        for other in ("ab", "abd", "zzz"):
            with self.assertRaises(ProofError):
                verify_proof(dict(proof, key=other), self.root)


class TestStagedRefusal(unittest.TestCase):
    def test_staged_key_is_refused_until_committed(self):
        rs, _ = populated()
        rs.put("fresh", 1)
        with self.assertRaises(ValueError) as ctx:
            rs.prove("fresh")
        self.assertIn("commit", str(ctx.exception))
        # other keys still prove: their committed truth is unaffected
        self.assertEqual(verify_proof(rs.prove("abc"), rs.root), "deep")
        rs.commit()
        self.assertEqual(verify_proof(rs.prove("fresh"), rs.root), 1)


class TestAddressing(unittest.TestCase):
    def test_unknown_store_needs_explicit_addressing(self):
        class Duck:  # duck-typed store with opaque addressing
            def __init__(self):
                self.inner = MemoryBytesStore()

            def put(self, data):
                return self.inner.put(data)

            def get(self, ref):
                return self.inner.get(ref)

        rs = RecordStore(Duck())
        rs.put("a", 1)
        rs.commit()
        with self.assertRaises(ValueError) as ctx:
            rs.prove("a")
        self.assertIn("addressing", str(ctx.exception))
        proof = rs.prove("a", addressing="sha256")   # the explicit override
        self.assertEqual(verify_proof(proof, rs.root), 1)

    def test_swarm_addressing_round_trips(self):
        # A store addressed with Swarm's own references (local BMT): the
        # proof names the scheme and verifies with it. Skips without swarmfs.
        import tempfile
        try:
            from recordstore import DirBytesStore
            store = DirBytesStore(tempfile.mkdtemp(), addressing="swarm")
            rs = RecordStore(store)
            rs.put("a", {"v": 1})
            rs.commit()
        except ImportError:
            self.skipTest("swarmfs not installed")
        proof = rs.prove("a")
        self.assertEqual(proof["addressing"], "swarm")
        self.assertEqual(verify_proof(proof, rs.root), {"v": 1})
        self.assertIs(verify_proof(rs.prove("nope"), rs.root), ABSENT)


class TestProofFuzz(unittest.TestCase):
    def test_random_histories_prove_in_and_out_against_a_dict_oracle(self):
        rnd = random.Random(20260801)
        for _ in range(6):
            rs = RecordStore(MemoryBytesStore())
            oracle = {}
            for _ in range(150):
                key = rnd.choice(KEY_POOL)
                if rnd.random() < 0.75 or key not in oracle:
                    value = rnd.choice(
                        [rnd.randint(0, 999), None, {"k": key}, [key]])
                    rs.put(key, value)
                    oracle[key] = value
                else:
                    rs.delete(key)
                    del oracle[key]
            rs.commit()
            for key, value in oracle.items():
                self.assertEqual(verify_proof(rs.prove(key), rs.root),
                                 value, key)
            for key in rnd.sample(KEY_POOL, 15):
                if key not in oracle:
                    self.assertIs(
                        verify_proof(rs.prove(key), rs.root), ABSENT, key)
            # and always: proofs survive the wire
            some = rnd.choice(sorted(oracle))
            self.assertEqual(
                verify_proof(json.loads(json.dumps(rs.prove(some))),
                             rs.root),
                oracle[some])


if __name__ == "__main__":
    unittest.main()
