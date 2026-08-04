# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [0.18.1] — 2026-08-04

### Changed

- The `[local]` extra's floor is now `swarmfs>=0.7.0`: no recordstore
  code changed, but 0.7.0 carries swarmfs's fix for the transaction-exit
  crash on fsspec < 2024.3.0 — the vintage Debian/Ubuntu LTS systems
  actually run via dist-packages, which pip never upgrades when the old
  version already satisfies a floor. Raising the floor is how existing
  installs pick the fix up.

## [0.18.0] — 2026-08-04

### Added

- **Working-set controls on local-first stores** (R2):
  `store.pin(name, prefix)` holds every blob needed to read the keys
  under a prefix — trie nodes and values, one subtree walk — against
  eviction ("always keep `users/` on this device"); `unpin(name)`
  releases. `store.fetch(prefix)` is the warm-up verb ("make me
  offline-capable before the flight") — heals everything under the
  prefix back from Swarm, idempotently.
- **Publication follows confirmation** (R2): `store.publish(pointer)`
  points a `SwarmFeedPointer` (or any pointer) at the newest
  **network-confirmed** root on the current lineage — never at content
  the network cannot serve yet; falls back to the nearest confirmed
  ancestor while the head is still syncing. `local_first_store(...,
  publish_pointer=...)` auto-publishes on each confirmation (failed feed
  updates retry on the next one).
- **History retention** (R3): `store.squash_history()` collapses this
  replica's lineage to the current root — recordstore walks the trie and
  re-lists the tip's full reachable set (the blob-blind journal layer
  cannot), the journal is rebased onto it, and dropped history's
  exclusive blobs are garbage-collected from disk. The explicit answer
  to "unpushed old roots are pinned forever"; dropped unpushed history
  is gone for good, pushed history stays on Swarm. Needs swarmfs ≥ 0.6.

### Changed

- **`RecordUnavailable` replaces the lying `KeyError`** (R2): when a key
  *exists* under the committed root but its value bytes are unreachable
  (evicted locally and offline; blob missing from the backend),
  `get`/`contains` now raise `RecordUnavailable` — deliberately not a
  `KeyError` subclass, so `except KeyError` and `contains()` can never
  misread "temporarily unreachable" as "absent". Previously the
  backend's `KeyError` leaked through and `contains` answered a wrong
  `False`.

## [0.17.0] — 2026-08-04

### Added

- **Local-first stores** (`local_first_store(path, api_url=None, ...)` →
  `LocalFirstRecordStore`): commits land on local disk instantly and are
  recorded — with their exact new-blob lists, trie nodes classified as
  eviction-priority `structure` — in a swarmfs localstore journal (the
  reflog); a background syncer pushes them to Swarm and confirms arrival
  peer-to-peer (Bee's stewardship retrieves through the network, verified
  from source), `sync()` is the blocking certainty barrier, and local disk
  is a budgeted working set: unpushed data is pinned (soft limit), only
  Swarm-confirmed blobs evict, and reads of evicted blobs heal by verified
  re-fetch. Offline is the normal mode — pulling the network mid-workload
  never blocks a commit. A `HEAD` pointer file keeps the current root
  across reopens (canonical addressing means returning to a prior state
  re-uses its old root, which the append-only journal refuses to
  duplicate). Requires swarmfs >= 0.5 (`recordstore[local]`); design in
  swarmfs `docs/localstore-design.md`, phases in ROADMAP (R1).
  Generic seam: any BytesStore exposing `commit_root`/`has_root` gets the
  journal integration — `commit()` records blobs through internal
  recording wrappers, so merge/reconcile writes are captured too.
- **`CachedBytesStore(inner, max_bytes=64MB)`** — byte-budgeted in-memory
  LRU in front of any backend; value blobs were previously re-fetched on
  every read. Unknown attributes delegate to the inner store. (R0)

### Changed

- The decoded trie-node cache is now bounded
  (`RecordStore(node_cache_size=65536)` default) instead of growing
  without limit; commit-scoped pending placeholders are exempt from
  eviction, so hostile bounds never break a commit. Stores larger than
  RAM iterate flat. (R0)

## [0.16.0] — 2026-08-01

### Added

- **Verifiable inclusion and absence proofs**: `RecordStore.prove(key)` →
  a self-describing, JSON-ready envelope (`{format, version, addressing,
  root, key, present, nodes, value}`) carrying the RAW trie-node blobs
  along the key's one possible path; module-level
  `verify_proof(proof, root)` checks it with **no store access** — pure
  hash-chain recomputation over the carried bytes, never re-serialization —
  returning the record for an inclusion proof or the `ABSENT` sentinel for
  an absence proof, raising `ProofError` on any mismatch. The canonical
  encoding is what makes *absence* provable: a key has exactly one place it
  could live under a given root, so exhibiting the path where the walk dies
  is authoritative. Proofs are O(depth) small, name their addressing scheme
  (`sha256`/`swarm`, auto-detected, `addressing=` to override), refuse keys
  with staged uncommitted changes (proofs are statements about committed
  roots), and are self-verified before being returned — so e.g. a Bee node
  whose erasure coding makes server refs diverge from plain content
  addresses fails loudly at prove time, not silently at the verifier.
  New exports: `verify_proof`, `ProofError`, `PROOF_FORMAT`. Requested by
  ontodag's CONTRACT.md §7 Tier 2 (the substrate for its `is_below`
  certificates and the factbond dispute rung).

## [0.15.0] — 2026-08-01

### Added

- **`RecordStore.diff(other_root)`** — "what changed between two published
  versions?" as a first-class question: yields `(key, mine, theirs)` per
  differing key, `ABSENT` for a side that lacks the key (so `None` values
  stay distinguishable), values decoded like `get`. The public face of the
  structural trie diff `merge` already used internally: shared subtrees are
  pruned by reference equality, so cost is proportional to the difference —
  equal roots read zero blobs. Compare arbitrary roots via
  `RecordStore.at(a, blobs).diff(b)`. Requested by ontodag's
  `MERKLE_NOTES.md` (no longer a prerequisite for its lazy writer, which
  shipped without it — this is the standalone "cheap version comparison"
  half of that note's recommendation).

## [0.14.0] — 2026-07-29

### Added

- **Postage batch health, surfaced where it bites.** `"auto"` selection now
  requires a day of remaining validity (`AUTO_MIN_BATCH_TTL`, per-store
  `min_batch_ttl=`) instead of swarmfs's 60-second floor, which is written for
  a one-shot upload: a batch with a minute left would have been selected and
  everything written under it would have stopped being paid for a minute later.
- **Two warnings on selection**, both silent failure modes until now: under a
  week of validity left (renew — an expired batch cannot be revived, and the
  chunks it paid for become the first eviction candidates), and a fullest
  bucket ≥ 80% full on an immutable batch (dilute — further chunks hashing
  there are refused even though the batch has capacity elsewhere).
- **`batch_status(api_url, batch_id)` and `BeeBytesStore.batch_status()`** —
  read-only batch health, `(StampInfo, BucketStats | None)`. `buckets=True`
  fetches the node's exact per-bucket histogram (~2 MB) instead of the
  summary. Cron it to learn that renewal is needed while renewal is still
  possible.

### Changed

- **A 402 on write now says which 402 it is.** `BeeBytesStore` owns its
  `requests` transport, so it does not inherit swarmfs's handling. `batch is
  overissued` (a full bucket) now explains that nothing already stored is
  lost and that diluting one depth then retrying fixes it; any other 402
  points at the batch and notes that an expired one cannot be revived.
  Previously both surfaced as a bare `402 Client Error`.
- **`swarmfs >= 0.4.0`** for the optional stamp path, declared as a new
  `[stamps]` extra (the inspection surface — `StampManager.list_batches`,
  `.buckets`, `StampInfo.bucket_capacity` — does not exist earlier, and an
  old swarmfs now fails with a version-specific message instead of an
  `AttributeError`).

Renewal itself is still deliberately absent: a library must not spend the node
wallet's xBZZ on its own. recordstore reports; the caller decides.

## [0.13.2] — 2026-07-28

### Changed

- Metadata only, no code changes. `requires-python` is now `>=3.11` (it declared
  `>=3.9`, which nothing tested), and `[project.urls]` is populated, so the PyPI
  page links back to the repository and issues. Published 0.13.1 and earlier
  carry the old metadata.

## [0.13.1] — 2026-07-28

### Added

- **`RecordStore.blobs`** — public accessor for the underlying bytes store, so
  a caller holding one store can open *another* root over the same backend
  (`RecordStore.at(other_root, store.blobs)`) without having kept a separate
  reference to it. Needed by OntoDAG's `merge_published`, which opens a
  published root over the blobs it already has.

## [0.13.0] — 2026-07-28

### Added

- **`DirBytesStore(path, addressing="sha256")`** — durable content-addressed
  blobs in a local directory, closing a real gap: `MemoryBytesStore` forgets
  everything on exit and `BeeBytesStore` needs a node and a postage batch, so
  there was previously no way to keep a versioned store on ordinary disk
  (`FilePointer` persists only the *root reference*, not the blobs). Writes are
  atomic (temp file + `os.replace`) and idempotent; names fan out two hex
  characters deep so a large store stays listable.
- **`FsspecBytesStore(url, addressing="sha256")`** — the same contract over any
  fsspec filesystem (local, S3, GCS, Azure, HTTP, SFTP, `memory://`). It
  **refuses `bzz://`/`bzzf://` explicitly**: fsspec is path-addressed while a
  Swarm reference is produced *by* the write, so pointing it at Swarm would
  store blobs at `<sha256>` paths and discard Swarm's own addressing — use
  `BeeBytesStore` or `swarm_store` instead. Needs the new `[fsspec]` extra.
- **Pluggable addressing** on both: `"sha256"` (default — roots stay portable
  with `MemoryBytesStore`), `"swarm"` (name blobs by their Swarm reference,
  computed locally via `swarmfs.splitter`, making a directory an offline mirror
  of Swarm's address space — new `[swarm-addressing]` extra), or any
  `bytes -> str` callable.

## [0.12.1] — 2026-07-28

### Fixed

- **The PyPI project page was blank.** `pyproject.toml` never declared
  `readme`, so the published metadata carried no long description at all —
  every release so far rendered an empty page. Now `readme = "README.md"`,
  matching its sibling packages.

## [0.12.0] — 2026-07-25

### Added

- **`swarm_store(topic, *, signer=... | owner=...)`** — the one call that puts a
  whole store on Swarm: blobs in a Bee node (`BeeBytesStore`) *and* the mutable
  latest-root in a Swarm feed (`SwarmFeedPointer`), so a published store has a
  stable address instead of a root hash passed around by hand. Previously
  callers assembled the two themselves, and the obvious wiring
  (`RecordStore(BeeBytesStore(...), FilePointer(...))`) left the head on local
  disk while only the blobs went to Swarm. The postage batch is resolved once
  by the blob store and shared with the feed's SOC writes. Everything above
  `RecordStore` stays backend-neutral; this is the single greppable answer to
  "where is Swarm specified?".

## [0.11.0] — 2026-07-24

### Added

- **`BeeBytesStore(postage_batch_id="auto")`** — the batch id now
  defaults to `"auto"`, which selects the node's usable batch with the
  longest remaining validity via swarmfs's `StampManager` (a lazy,
  optional import in the spirit of the `requests` boundary; passing an
  explicit id keeps recordstore swarmfs-free). Selection only, never
  purchase: spending the node wallet's xBZZ stays a deliberate caller
  action — swarmfs's `StampManager.plan`/`buy` is the programmatic way
  to buy, documented in the User Guide. Verified live: `"auto"` picked
  the freshest batch on a real node and round-tripped a blob.

## [0.10.0] — 2026-07-20

### Added

- **`SwarmFeedPointer.compare_and_set(expected, new)`** — makes
  `commit(reconcile=True)` work across processes over a shared Swarm feed. It
  reads the feed head *fresh* (bypassing the read-your-writes/TTL cache, so it
  reliably detects a feed another writer already advanced) and verifies its own
  write read-back. It is **best-effort, not atomic**: a Swarm feed has no
  index-claim primitive — Bee accepts and overwrites a second update at the same
  index (confirmed against 2.8.1) — so two writers committing at the exact same
  index simultaneously can still race. It narrows the window to near-
  simultaneous collisions rather than any concurrent write. Verified live: two
  writers over one feed converge (`test_reconcile_over_feed`).

## [0.9.0] — 2026-07-20

### Added

- **`commit(reconcile=True, resolver=None, retries=5)`** — auto-reconciling
  commit. With a pointer attached, if the pointer moved past the root this
  commit built on, the two versions are three-way merged and the commit is
  retried until it lands, so concurrent writers converge instead of clobbering
  each other. Plain `commit()` is unchanged (last-write-wins).
- **`MemoryPointer.compare_and_set(expected, new)`** — atomic in-process CAS;
  `commit(reconcile=True)` uses it for race-free updates, and falls back to a
  best-effort read-then-set for pointers without one.

### Changed

- **`RecordStore.merge` is now O(divergence), not O(dataset), on both sides.**
  The read uses a structural trie diff that prunes subtrees with equal refs
  (canonical roots make equal content share a ref), so merging a single-key
  difference in a 1000-record store touched ~15 blobs instead of ~2000. The
  write already applied only the diff. Behaviour is unchanged — validated by
  new fuzz tests comparing the diff and the full merge to brute-force oracles
  over hundreds of random cases.

## [0.8.0] — 2026-07-20

### Added

- **`RecordStore.merge(bytes_store, base, ours, theirs, resolver=None)`** —
  canonical three-way merge of two roots that diverged from a common `base`,
  returning the merged root. A change on one side is taken; the same change on
  both sides is taken once; different changes to the same key conflict. By
  default conflicts raise **`MergeConflict`** (`.conflicts` lists the keys) —
  nothing is dropped silently — or a `resolver(key, base, ours, theirs)` settles
  them (each arg is the value or the `ABSENT` sentinel; return a value or the
  `DELETE` sentinel). Reference equality makes unchanged subtrees merge for
  free, and only the merged diff is written (shared with `base`). Commutative
  when the resolver is symmetric in ours/theirs (the default raise is). This is
  the primitive for multi-writer reconciliation over a `SwarmFeedPointer`.
- New exports: `MergeConflict`, `ABSENT`, `DELETE`.

## [0.7.1] — 2026-07-20

### Changed

- CPU micro-optimizations on the hot paths, no behaviour change: `put()` now
  canonically encodes each value once rather than twice (it was validating and
  detaching in separate encodes), and the trie insert path uses a leaner
  byte-prefix helper instead of `os.path.commonprefix`. ~13% less CPU on a
  build+commit+hydrate of 5000 records (profiled over `MemoryBytesStore`).
  Negligible for network-bound use — where round trips dominate — but useful at
  scale and with the in-memory backend. Roots and results unchanged (fuzz +
  full suite green).

## [0.7.0] — 2026-07-20

### Changed

- **`commit()` now writes the trie in bulk instead of one key at a time.** Node
  writes are buffered during the insert/delete build, then flushed bottom-up one
  level at a time via `put_many` — children before parents, since the backend
  assigns each node's reference from its children's. Two wins: only the nodes
  surviving in the final root are written (orphaned intermediates from
  sequential insertion are pruned — a 20-record commit dropped from 71 blob
  writes to 43), and each level is one concurrent batch, so a commit costs
  O(trie depth) round-trip rounds instead of O(nodes) serial puts. The resulting
  root is byte-identical to before — guarded by the fuzz oracle and the
  batched-vs-incremental root-equality test. No public API change.

## [0.6.0] — 2026-07-20

### Changed

- **`BeeBytesStore` now reuses a pooled HTTP session** instead of opening a
  fresh connection per blob op. Keep-alive removes a TCP (and TLS) handshake
  from every read and write — the dominant per-op cost on a high-latency link —
  and gives concurrent reads a pool of reusable connections. Locally this made
  bulk reads ~8× faster (and fixed a case where the read concurrency was
  *slower* than serial because every parallel request opened a cold
  connection); the worse the link, the larger the gain. Pool size follows
  `max_concurrent_reads`.
- **`commit()` uploads value blobs concurrently.** A commit's value blobs are
  independent, so they are written in one batch up front rather than one serial
  round trip each interleaved with the trie build. Trie node writes stay
  sequential — a parent node's reference depends on its children's
  server-assigned refs. The resulting root is unchanged.

### Added

- Optional **`BytesStore.put_many(datas)`** — batch upload, mirroring
  `get_many`. `BeeBytesStore` runs it concurrently; `MemoryBytesStore` serially.

## [0.5.1] — 2026-07-20

### Changed

- `keys()` and `items()` no longer buffer the whole result set and sort it at
  the end. The trie is walked in sorted pre-order (a node's key precedes its
  descendants', children in byte order), so keys stream out already sorted:
  `keys()` merges the staged overlay lazily, and `items()` fetches value blobs
  in windows (bounded by `max_concurrent_reads`) so memory stays flat on large
  result sets while the reads still parallelise within each window. Each node's
  children are still prefetched in one batch, preserving the 0.5.0 read
  concurrency. Iteration order and results are unchanged — validated by the
  fuzz suite. No API change.

## [0.5.0] — 2026-07-20

### Added

- **`RecordStore.items(prefix="")`** — sorted `(key, value)` pairs with the
  committed value blobs fetched in one batch. Over a network store this
  parallelises the reads instead of paying one serial round trip per record,
  so hydrating a whole store (or a prefix) is dramatically faster on a
  high-latency link. Staged overlay included; values deep-copied like `get`.
- **Optional `BytesStore.get_many(refs)`** — batch read. `MemoryBytesStore`
  implements it trivially; `BeeBytesStore` fetches concurrently via a thread
  pool (`max_concurrent_reads`, default 16). Reads need no locking — everything
  below `RecordStore` is immutable and content-addressed. Trie traversal now
  loads each level through `get_many`, so prefix scans parallelise too. Stores
  without `get_many` fall back to serial `get` (the protocol's required
  contract is still just `put`/`get`).

## [0.4.1] — 2026-07-20

### Changed

- **`SwarmFeedPointer` index discovery no longer depends on the flaky /feeds
  lookup.** The tip index is found by probing the feed's SOC chunks directly
  (exponential + binary search over `download_soc`), which are individually
  retrievable even when the /feeds lookup 404s on a high-latency link — the
  failure mode reported in ethersphere/bee#5251. This makes *cold* reads (a
  fresh reader with no cached index) reliable in a single attempt, and lets
  `set()` place the next index correctly without a network lookup when it
  already has a floor (single-writer model).
- The warm read path additionally tries Bee's `after` index hint
  (`GET /feeds/{owner}/{topic}?after=N`) first — one round trip, resuming just
  below the tip — and falls back to the SOC probe when it flakes. swarm-bee's
  typed API does not expose `after` (see bee-py#2), so it is sent through the
  client transport, guarded by a capability check. Verified live on Bee 2.8.1
  (`?after=N` resolves where the plain lookup 404s). No public API change.

## [0.4.0] — 2026-07-20

### Added

- **`SwarmFeedPointer`** — the `Pointer` "latest root" backed by an owner-signed
  Swarm feed (previously a stub that raised `NotImplementedError`). `set(root)`
  publishes a signed single-owner chunk; `get()` resolves the latest via a feed
  lookup. Built on the `swarm-bee` package for BMT/secp256k1 signing, behind a
  new `recordstore[feeds]` extra and imported lazily so the core stays
  stdlib-only. Because Swarm feed lookups are unreliable per call on a light
  node, it uses a read-your-writes cache, a monotonic write-index floor, and
  retry-until-stable reads with a stale-early guard (policy follows swarmfs's
  `bzzf://` layer). Constructor exposes `feed_ttl` / `max_lookup_retries` /
  `retry_backoff` knobs. Accepts a `signer` (read+write) or an `owner`
  (read-only).
- `tests/test_recordstore_feed.py` — env-gated live-node integration test for
  the feed pointer (skips unless `BEE_API` is set and `swarm-bee` is installed).

## [0.3.0] — 2026-07-20

### Changed

- **Breaking:** renamed the storage abstraction `ChunkStore` → `BytesStore` and
  its in-memory implementation `MemoryChunkStore` → `MemoryBytesStore`. "Chunk"
  collided with Swarm's own chunk primitive (the fixed-size unit at
  `/chunks/{address}`), whereas a recordstore storage unit is a
  `put(bytes) → ref` blob that Bee's `/bytes` endpoint splits into a *tree* of
  Swarm chunks — so the old name implied something untrue. The `RecordStore(...)`
  and `RecordStore.at(...)` store parameter is likewise renamed `chunks` →
  `bytes_store`. `BeeBytesStore` (renamed in 0.2.0) is unchanged. Internal and
  documentation vocabulary now says "blob" for a stored unit and "bytes store"
  for the layer; genuine references to Swarm/prolly-tree/IPFS chunks are
  retained.

### Removed

- Untracked `src/recordstore.egg-info/` — a gitignored build artifact that had
  been committed by mistake.

## [0.2.0] — 2026-07-19

### Changed

- **Breaking:** renamed `BeeChunkStore` → `BeeBytesStore`, reflecting that it
  uses Bee's `/bytes` blob endpoint rather than the raw `/chunks/{address}`
  single-chunk primitive.
