# recordstore roadmap

Forward-looking, multi-phase tracks. Near-term limitations and their
incremental fixes live in the user guide's
[§7 Limitations and roadmap](docs/USER_GUIDE.md#7-limitations-and-roadmap);
this file is for larger bets that span several releases.

---

## Local-first sync track (designed 2026-08-04; R0+R1 landed 2026-08-04)

**Status: shared layer L0+L1 shipped in swarmfs (property-tested,
live-validated against Bee 2.8.1, confirmation p2p-native via stewardship
— verified from the Bee source); recordstore R0 (bounded caches) and R1
(`local_first_store`) released as 0.17.0 with swarmfs 0.5.0. R2
(partial-replica controls + publication-after-confirmation) and R3
(history retention via app-assisted squash) landed 2026-08-04 — in
CHANGELOG [Unreleased], pending a swarmfs 0.6 release (`rebase_root`/
`gc_orphans`). The track is feature-complete as designed; what remains
lives in swarmfs (L4: scrub, only-on-Swarm accounting in status()).**
Canonical design document: `../swarmfs/docs/localstore-design.md`
(invariant, durability ladder, on-disk format, eviction policy, phases
L0–L4). This section tracks only what changes *in recordstore*.

The bet: stop making users choose one home for their blobs. Today a store
lives in memory (forgets), on disk (doesn't publish), or on a Bee node
(every read a round trip; every commit a network and postage liability, and
a "successful" deferred upload still only means *the node* has it). The fix
is git-shaped, not cache-shaped: commits always land on local disk
instantly (offline is the normal mode), a background worker pushes to Swarm
and *confirms* arrival, and local storage is a budgeted working set in
which unpushed data is pinned and only network-confirmed data is evictable.
The performance story rides along: local reads, push coalescing (orphaned
intermediate blobs never uploaded — postage saved), offline BMT addressing.

recordstore is already most of git — content-addressed blobs, structural
sharing, `diff`, three-way `merge`, `Pointer` refs, and
`DirBytesStore(addressing="swarm")` as the offline mirror. What it lacks is
transfer verbs and recorded lineage; both come from the shared layer.

### Guardrails

- **`BytesStore` stays the contract.** The shared layer implements it; no
  public-API break. Roots stay canonical — history/parentage lives in the
  journal (the layer's reflog), **never** inside roots, or canonical
  addressing and merge's ref-equality pruning die.
- **`commit()` stays local-fast and never blocks on network or stamps.**
  Certainty is explicit: `sync()` barrier + `status()` ladder
  (committed → on-node → network-confirmed).
- **The journal lags reality, never leads it** — bookkeeping may
  under-claim durability, never over-claim. Only network-confirmed blobs
  become evictable.
- **"Confirmed" means retrieve-and-verify, not a node's claim.** The rung
  that unlocks eviction — deleting the local copy — must be backed by
  fetching (a sample of) the blobs back through the network and checking
  them against their references; tags/stewardship responses alone promote
  no further than on-node. Eviction safety is this store's data safety, so
  recordstore does not adopt the layer without this property (design doc,
  *Verification and trust*).
- The shared layer never interprets blobs: recordstore supplies new-blob
  lists at commit, priority hints (trie nodes = structure, values = data),
  and pin-refs from its own subtree walks.

### Phases

- **R0 — Bounded caches. ✅ 2026-08-04** (CHANGELOG [Unreleased]). The in-memory value-blob cache (`RecordStore.get`
  currently re-fetches values on every read) and a bound on the
  currently-unbounded `_Trie._cache`, both as byte-budgeted LRU. Shaped as
  a wrapping `BytesStore` so it composes with any backend.
  *Acceptance:* a stores-larger-than-RAM iteration test holds memory flat.
- **R1 — Adopt `swarmfs.localstore`. ✅ 2026-08-04** — `local_first_store()`
  / `LocalFirstRecordStore`; both acceptance tests hold (cable-pull and
  DirBytesStore commit-latency parity). *Findings:* a `HEAD` pointer file
  is required beside the journal — canonicity means returning to a prior
  state re-uses its root, which the append-only journal refuses to
  re-record, so `latest_root()` alone misreports the head; and an emptied
  store (root `None`) cannot be journaled at all (no null-root event in
  the format) — HEAD carries that case too. Feed publication is *not*
  wired (belongs after confirmation → R2). Original plan for reference:
  Local-first commit to a store directory; auto-push on by default;
  `sync()`, `status()`, push/pull/fetch verbs; the journal doubles as the
  reflog, giving cross-session merge-base discovery (today `_reconcile`
  only knows the base in-process). `BeeBytesStore`/`DirBytesStore` remain
  as thin adapters or direct-mode escape hatches.
  *Acceptance:* pull the network cable mid-workload — commits keep
  succeeding; reconnect — `sync()` returns and stewardship confirms every
  root. And no local regression: a localstore-backed commit at the default
  durability (commit-boundary fsync batching, not L0's per-blob fsync) is
  not meaningfully slower than `DirBytesStore` for a many-small-node
  commit.
- **R2 — Partial-replica controls. ✅ 2026-08-04** — `pin(name, prefix)` /
  `unpin` / `fetch(prefix)` over the new `_Trie.refs_under` walk;
  `RecordUnavailable` (deliberately not a `KeyError`) for
  exists-but-unreachable values, so `contains` can never answer a wrong
  `False`; `publish(pointer)` + `publish_pointer=` auto-publication that
  strictly follows network confirmation (nearest confirmed ancestor while
  the head syncs). Structure-resident/values-remote needed no new code —
  it is the eviction ordering (payload before structure) doing its job;
  batch-TTL surfacing in `status()` stays with swarmfs L4.
- **R3 — History retention. ✅ 2026-08-04** — `squash_history()`: the
  trie walk re-lists the tip's full reachable set (only the app can — the
  journal layer is blob-blind), swarmfs ≥ 0.6's `rebase_root` collapses
  the lineage onto it, `gc_orphans` frees dropped history's exclusive
  blobs. Dropped *unpushed* history is gone for good (that is the
  explicit retention decision); pushed history stays on Swarm.
  Policy layers (age-based, count-based) can build on the primitive
  later if wanted; the default remains keep-everything — confirmed
  history costs almost nothing locally because it evicts.

### References

- Design doc (canonical): `../swarmfs/docs/localstore-design.md`.
- swarmfs roadmap: `../swarmfs/docs/roadmap.md` §v3.
- Seams this builds on: `BytesStore` protocol, `_Trie._cache`,
  `DirBytesStore(addressing="swarm")`, `RecordStore.merge`/`diff`,
  `Pointer` — all in `src/recordstore/recordstore.py`.

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

*Update 2026-08-01:* the radix trie now has **native** inclusion/absence
proofs (`RecordStore.prove` / `verify_proof`, v0.16.0) — the off-chain half
of the proof story, verifiable by anyone holding the root, no store access.
What the POT track would still add is the *on-chain* half (the Solidity
`POTProofVerifier`) and wire-format interop; a future C-track item should
define the proof interface so both encodings serve it.

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
