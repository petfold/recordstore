# recordstore roadmap

Forward-looking, multi-phase tracks. Near-term limitations and their
incremental fixes live in the user guide's
[§7 Limitations and roadmap](docs/USER_GUIDE.md#7-limitations-and-roadmap);
this file is for larger bets that span several releases.

---

## Canonical-POT convergence track (experimental)

**Status: experimental / research. Not scheduled against a release.**

POT (Proximity Order Trie; Trón & Verbin) is an authenticated index for Swarm:
a 256-bit occupancy bitmap + packed 32-byte fork references + a pinned key and
value (inline or by reference) per node, with proximity-order (longest-common-
prefix) branching. As published it is *non-canonical* — the root depends on
update history. A **canonical variant** — pin, treap-style, the element with
minimal `H(key)` at the top of every subtree — would make the root a
deterministic function of the key set, exactly like recordstore's radix trie.
That variant is a design sketch only; it needs no wire-format change, just a
construction discipline plus a merge algorithm.

The bet: prototype the canonical variant here in Python, behind recordstore's
existing API, as a *second* index encoding. If it holds up, propose the pin
rule + merge algorithm upstream with the prototype as evidence. If upstream
adopts and *freezes* it (with published conformance vectors), recordstore could
later swap its internal encoding to the POT wire format and inherit POT's proof
system, Solidity verifier, and cross-language interop — public API unchanged.
If upstream never adopts, the radix trie stays and the work below still pays for
itself (see *Standalone value*).

### Guardrails

- The radix trie remains the **default and only production encoding** until
  further notice; the POT encoding is experimental and clearly marked so.
- **No public API changes** in service of this track.
- **Do NOT** swap recordstore's default internals to the POT wire format
  unless/until the canonical variant is frozen upstream with published
  conformance vectors. Adopting a moving format is inheriting someone else's
  churn.
- If upstream never adopts: keep the radix trie, keep the merge work, archive
  the prototype without ceremony.

### Phases

- **C0 — Index seam.** Establish a clean internal boundary:
  public API → index-encoding layer → chunk store.
  *Acceptance:* a second index encoding can be registered without touching the
  public API (`RecordStore`) or the chunk store (`BytesStore`).
  *Current state (verified in code, not yet met):* the **chunk-store** seam is
  clean — `BytesStore` is a `Protocol` injected into both `RecordStore` and the
  trie, with `MemoryBytesStore`/`BeeBytesStore` implementations. The **index**
  seam is not a registration point: `_Trie` is a concrete private class
  hard-wired in `RecordStore.__init__` (and again in `RecordStore.merge`), and
  `RecordStore` reaches into its partly-private surface (`_diff`, `_flush`,
  `_buffering`, `_reset_buffer`). C0 is the work of promoting that implicit
  contract into an injectable index interface — a refactor with no behavioural
  or API change, provable by keeping the existing suite green.

- **C1 — Canonical-POT prototype.** Implement POT node semantics (bitmap fork
  table, one pinned element per node, proximity-order = longest-common-prefix
  branching) as an experimental index encoding, with the hash-derived
  pin-priority rule enforcing canonical shape.
  *Acceptance (property tests are the point):* same key set → byte-identical
  root regardless of insertion order; insert/delete round-trips preserve
  canonicity. Reuse the fuzz suite's dict-oracle pattern.

- **C2 — Merge/diff.** Subtree-hash short-circuit merge and diff over the
  canonical structure.
  *Reconciliation:* this already exists **for the radix trie** —
  `RecordStore.merge` + `_Trie._diff`/`_diff_nodes`, short-circuiting unchanged
  subtrees by root-ref equality, O(divergence). C2 is therefore (a) lift that
  merge to the C0 index interface so it is defined generically rather than baked
  into `_Trie`, and (b) implement it for the POT encoding.
  *Acceptance:* one merge interface, exercised by both encodings.

- **C3 — Conformance.** Test vectors against the Go implementation's wire format
  where obtainable; where not, generate and commit our own vectors and document
  every known or suspected divergence from the Go encoding explicitly. Silence
  about a divergence is a bug.

- **C4 — Upstream proposal.** A short spec document — pin rule, merge algorithm,
  edge cases found, prototype results — suitable for filing as a discussion/issue
  on `ethersphere/proximity-order-trie`. **Document deliverable, not code.**

### Standalone value (holds even if convergence never happens)

- **C0** proves the API is genuinely index-agnostic: recordstore's abstraction
  claim gets tested by a real second implementation rather than asserted.
- **C2**'s generalized merge/diff is directly useful to the radix trie and to
  multi-writer OntoDAG scenarios regardless of POT.
- The **prototype** doubles as an executable reference for the upstream
  proposal — ecosystem value for Swarm independent of this repo's internals.

### References

- Public API / chunk-store seam: `src/recordstore/recordstore.py` —
  `RecordStore`, `BytesStore`, `_Trie`.
- Canonicity contract and background:
  [user guide](docs/USER_GUIDE.md).
- POT reference implementation (external): `github.com/ethersphere/proximity-order-trie`
  (v1.0.0); wire-compatible JS/TS ports `potjs`, `@snaha/swarm-pot`.
