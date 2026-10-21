"""Prefix proofs (RecordStore.prove_prefix / verify_prefix).

"Under this prefix, root R holds exactly these records": the properties
that matter:

  P1  The proof returns every record under the prefix, with no store
      access, and survives the wire; "" is the whole store.
  P2  It is complete: a dropped node or value blob is refused, so a record
      cannot be left out of the answer.
  P3  It is adversarial: an altered node, another root, another prefix, a
      wrong format — each raises ProofError.
  P4  Empty stores, empty prefixes and staged keys behave.
  P5  Across random stores, the returned records equal a dict oracle's.
"""

import json
import random
import unittest

from recordstore import (MemoryBytesStore, ProofError, RecordStore,
                         verify_prefix)

KEY_POOL = (
    ["a", "ab", "abc", "abcd", "abcdef", "abd", "b", "ba", "bab"]
    + ["in(" + s + ")" for s in ("alps", "al", "japan", "日本")]
    + [f"k{i:02d}" for i in range(12)]
)


def store(keys):
    rs = RecordStore(MemoryBytesStore())
    for i, key in enumerate(keys):
        rs.put(key, {"n": i, "key": key})
    return rs, rs.commit()


class TestPrefix(unittest.TestCase):
    def test_every_record_under_the_prefix_without_a_store(self):
        rs, root = store(KEY_POOL)
        proof = rs.prove_prefix("in(")
        expected = {k: rs.get(k) for k in KEY_POOL if k.startswith("in(")}
        del rs
        self.assertEqual(verify_prefix(proof, root), expected)
        self.assertEqual(verify_prefix(json.loads(json.dumps(proof)), root), expected)

    def test_the_empty_prefix_is_the_whole_store(self):
        rs, root = store(KEY_POOL)
        self.assertEqual(verify_prefix(rs.prove_prefix(), root),
                         {k: rs.get(k) for k in KEY_POOL})

    def test_a_dropped_node_or_value_is_refused(self):
        rs, root = store(KEY_POOL)
        proof = rs.prove_prefix()
        for field in ("nodes", "values"):
            for i in range(len(proof[field])):
                cut = dict(proof, **{field: proof[field][:i] + proof[field][i + 1:]})
                with self.assertRaises(ProofError):
                    verify_prefix(cut, root)

    def test_tampering_is_refused(self):
        rs, root = store(KEY_POOL)
        proof = rs.prove_prefix("a")
        other, other_root = store(KEY_POOL[1:])
        with self.assertRaises(ProofError):
            verify_prefix(proof, other_root)
        with self.assertRaises(ProofError):
            verify_prefix(dict(proof, format="recordstore-trie-proof"), root)
        with self.assertRaises(ProofError):
            verify_prefix(dict(proof, version=2), root)
        # another prefix walks nodes the proof does not carry
        with self.assertRaises(ProofError):
            verify_prefix(dict(proof, prefix=""), root)
        blob = bytearray.fromhex(proof["nodes"][0])
        blob[-1] ^= 1
        with self.assertRaises(ProofError):
            verify_prefix(dict(proof, nodes=[blob.hex()] + proof["nodes"][1:]), root)

    def test_empty_cases_and_staged_keys(self):
        rs = RecordStore(MemoryBytesStore())
        self.assertEqual(verify_prefix(rs.prove_prefix(), None), {})
        rs, root = store(KEY_POOL)
        self.assertEqual(verify_prefix(rs.prove_prefix("zzz"), root), {})
        rs.put("in(x)", {"n": -1})
        with self.assertRaises(ValueError):
            rs.prove_prefix("in(")
        rs.prove_prefix("k")                   # staged keys elsewhere are fine
        with self.assertRaises(TypeError):
            rs.prove_prefix(None)

    def test_agrees_with_an_oracle(self):
        rng = random.Random(7)
        for _ in range(40):
            keys = rng.sample(KEY_POOL, rng.randint(0, len(KEY_POOL)))
            rs, root = store(keys)
            for prefix in ("", "a", "ab", "in(", "in(al", "k0", "q"):
                expected = {k: rs.get(k) for k in keys if k.startswith(prefix)}
                self.assertEqual(verify_prefix(rs.prove_prefix(prefix), root), expected)


if __name__ == "__main__":
    unittest.main()
