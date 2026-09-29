"""Extension proofs (RecordStore.prove_extension / verify_extension / extends).

"Under these prefixes, root B holds every record root A held, unchanged" —
additions allowed, nothing removed or altered. The properties that matter:

  E1  A root that only adds records under the prefixes extends its base;
      the proof verifies with no store access and survives the wire.
  E2  A record removed or changed under the prefixes is refused: `prove`
      raises naming the key, `extends` answers False.
  E3  Changes outside the prefixes do not matter; an empty base is extended
      by every root, and an empty root extends only a base with nothing
      under the prefixes.
  E4  Verification is adversarial: a dropped or altered node, a swapped
      base or root, an added prefix, a wrong format — each raises ProofError.
  E5  The proof's size follows the change, not the store.
  E6  Across random histories, `extends` agrees with a dict oracle for every
      pair of committed roots, and a proof exists exactly when it does.
"""

import json
import random
import unittest

from recordstore import (MemoryBytesStore, ProofError, RecordStore,
                         verify_extension)

KEY_POOL = (
    ["a", "ab", "abc", "abcd", "abcdef", "abd", "b", "ba", "bab"]
    + ["revoked/" + s for s in ("x", "xy", "xyz", "xz", "y", "日本")]
    + ["status/" + s for s in ("x", "xy", "y", "🐝")]
    + [f"k{i:02d}" for i in range(12)]
)


def register():
    """A register's book at two moments: a revocation added, a status
    changed, a key outside the prefixes deleted."""
    rs = RecordStore(MemoryBytesStore())
    for sid in ("aa", "ab", "b"):
        rs.put(f"status/{sid}", {"state": "issued"})
    rs.put("revoked/zz", {"at": 1})
    rs.put("heartbeat", {"at": 100})
    base = rs.commit()
    rs.put("revoked/ab", {"at": 2})
    rs.put("status/ab", {"state": "revoked"})
    rs.put("heartbeat", {"at": 200})
    rs.delete("status/b")
    later = rs.commit()
    return rs, base, later


class TestExtension(unittest.TestCase):
    def test_additions_under_the_prefix_extend_and_verify_without_a_store(self):
        rs, base, later = register()
        proof = rs.prove_extension(base, ["revoked/"])
        del rs
        self.assertEqual(verify_extension(proof, base, later), ("revoked/",))
        self.assertEqual(verify_extension(json.loads(json.dumps(proof)), base, later), ("revoked/",))

    def test_a_removed_or_changed_record_is_refused(self):
        rs, base, later = register()
        self.assertFalse(rs.extends(base, ["status/"]))       # status/ab changed, status/b gone
        with self.assertRaises(ValueError) as ctx:
            rs.prove_extension(base, ["status/"])
        self.assertIn("status/ab", str(ctx.exception))
        self.assertTrue(rs.extends(base, ["revoked/"]))
        # a later root that drops a revocation (an un-revocation) is not an extension
        rs.delete("revoked/zz")
        dropped = rs.commit()
        self.assertFalse(rs.extends(later, ["revoked/"]))
        with self.assertRaises(ValueError) as ctx:
            rs.prove_extension(later, ["revoked/"])
        self.assertIn("revoked/zz", str(ctx.exception))
        self.assertIn("absent", str(ctx.exception))
        self.assertTrue(rs.extends(dropped, ["revoked/"]))   # and the new state extends itself

    def test_outside_the_prefixes_nothing_matters_and_the_empty_cases(self):
        rs, base, later = register()
        self.assertTrue(rs.extends(base, ["revoked/", "status/aa"]))
        self.assertFalse(rs.extends(base, [""]))              # the whole store: status/b is gone
        self.assertTrue(rs.extends(None, [""]))               # the empty store is extended by all
        self.assertTrue(rs.extends(later, ["revoked/"]))      # a root extends itself
        empty = RecordStore(rs.blobs)                         # the empty root over the same blobs
        self.assertTrue(empty.extends(base, ["nothing/"]))
        self.assertFalse(empty.extends(base, ["revoked/"]))
        self.assertEqual(verify_extension(RecordStore.at(later, rs.blobs).prove_extension(None, [""]),
                                          None, later), ("",))

    def test_staged_keys_under_the_prefixes_are_refused(self):
        rs, base, later = register()
        rs.put("revoked/new", {"at": 3})
        with self.assertRaises(ValueError):
            rs.prove_extension(base, ["revoked/"])
        rs.prove_extension(base, ["status/aa"])               # staged elsewhere: fine


class TestAdversarial(unittest.TestCase):
    def setUp(self):
        self.rs, self.base, self.later = register()
        self.proof = self.rs.prove_extension(self.base, ["revoked/"])

    def test_a_dropped_node(self):
        for i in range(len(self.proof["nodes"])):
            bad = dict(self.proof, nodes=[n for j, n in enumerate(self.proof["nodes"]) if j != i])
            with self.assertRaises(ProofError):
                verify_extension(bad, self.base, self.later)

    def test_an_altered_node(self):
        for i, node in enumerate(self.proof["nodes"]):
            flipped = node[:-2] + ("00" if node[-2:] != "00" else "01")
            bad = dict(self.proof, nodes=[flipped if j == i else n for j, n in enumerate(self.proof["nodes"])])
            with self.assertRaises(ProofError):
                verify_extension(bad, self.base, self.later)

    def test_swapped_roots_and_an_added_prefix(self):
        with self.assertRaises(ProofError):
            verify_extension(self.proof, self.later, self.base)
        with self.assertRaises(ProofError):
            verify_extension(dict(self.proof, base=self.later, root=self.base), self.later, self.base)
        with self.assertRaises(ProofError):                   # a prefix the nodes do not cover
            verify_extension(dict(self.proof, prefixes=["revoked/", "status/"]), self.base, self.later)

    def test_a_wrong_format_or_version(self):
        with self.assertRaises(ProofError):
            verify_extension(dict(self.proof, format="recordstore-trie-proof"), self.base, self.later)
        with self.assertRaises(ProofError):
            verify_extension(dict(self.proof, version=2), self.base, self.later)
        with self.assertRaises(ProofError):
            verify_extension(dict(self.proof, prefixes=[]), self.base, self.later)


class TestSize(unittest.TestCase):
    def test_the_proof_follows_the_change_not_the_store(self):
        rs = RecordStore(MemoryBytesStore())
        for i in range(2000):
            rs.put(f"revoked/{i:06d}", {"at": i})
            rs.put(f"status/{i:06d}", {"state": "issued"})
        base = rs.commit()
        rs.put("revoked/999999", {"at": 1})
        rs.commit()
        proof = rs.prove_extension(base, ["revoked/"])
        self.assertLess(len(proof["nodes"]), 16)
        self.assertEqual(verify_extension(proof, base, rs.root), ("revoked/",))


class TestExtensionFuzz(unittest.TestCase):
    def test_random_histories_agree_with_a_dict_oracle(self):
        rnd = random.Random(20260929)
        for _ in range(5):
            rs = RecordStore(MemoryBytesStore())
            oracle, states = {}, []
            for _ in range(12):
                for _ in range(rnd.randint(1, 8)):
                    key = rnd.choice(KEY_POOL)
                    if rnd.random() < 0.8 or key not in oracle:
                        value = rnd.choice([rnd.randint(0, 9), None, {"k": key}])
                        rs.put(key, value)
                        oracle[key] = value
                    else:
                        rs.delete(key)
                        del oracle[key]
                states.append((rs.commit(), dict(oracle)))
            for (root_a, a), (root_b, b) in [(rnd.choice(states), rnd.choice(states)) for _ in range(20)]:
                for prefix in ("", "revoked/", "status/", "a", "k0", "nothing/"):
                    truth = all(k in b and b[k] == v for k, v in a.items() if k.startswith(prefix))
                    view = RecordStore.at(root_b, rs.blobs)
                    self.assertEqual(view.extends(root_a, [prefix]), truth, (prefix, a, b))
                    if truth:
                        proof = view.prove_extension(root_a, [prefix])
                        self.assertEqual(verify_extension(proof, root_a, root_b), (prefix,))
                    else:
                        with self.assertRaises(ValueError):
                            view.prove_extension(root_a, [prefix])

    def test_swarm_addressing_round_trips(self):
        import tempfile
        try:
            from recordstore import DirBytesStore
            store = DirBytesStore(tempfile.mkdtemp(), addressing="swarm")
            rs = RecordStore(store)
            rs.put("revoked/a", 1)
            base = rs.commit()
        except ImportError:
            self.skipTest("swarmfs not installed")
        rs.put("revoked/b", 2)
        rs.commit()
        proof = rs.prove_extension(base, ["revoked/"])
        self.assertEqual(proof["addressing"], "swarm")
        self.assertEqual(verify_extension(proof, base, rs.root), ("revoked/",))


if __name__ == "__main__":
    unittest.main()
