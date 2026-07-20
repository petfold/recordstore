# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

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
