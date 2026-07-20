# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

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
