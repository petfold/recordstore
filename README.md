# recordstore

A versioned key→record store over a content-addressed chunk store.

- `RecordStore`: staged put/get/delete, `commit() → root`, read-only snapshots via
  `RecordStore.at(root)`, sorted prefix iteration (`keys(prefix)`).
- Backed by a persistent, canonically-encoded compacted radix trie: **equal content
  produces equal roots**, independent of edit history (content-addressable, diffable,
  mergeable).
- `ChunkStore` backends: `MemoryChunkStore` (in-memory), `BeeChunkStore` (Ethereum
  Swarm Bee node over `/bytes`; requires `requests`, install with the `[bee]` extra).
- `Pointer` backends: `MemoryPointer`, `FilePointer` (atomic file-based root pointer),
  `SwarmFeedPointer` (documented stub — feed writes need client-side SOC signing).

Module-level imports are stdlib-only; `requests` is imported lazily inside
`BeeChunkStore` methods.

Extracted from [petfold/ontodag](https://github.com/petfold/ontodag) (July 2026) with
history preserved.

```bash
python3 -m pytest tests/                                 # unit + fuzz + boundary tests
BEE_API=http://<node>:1633 BEE_BATCH=<batchID> \
    python3 -m pytest tests/test_recordstore_bee.py -v   # live Bee node integration
```

```bash
pip install "recordstore @ git+https://github.com/petfold/recordstore.git@v0.1.0"
```
