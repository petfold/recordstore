# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

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
