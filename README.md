# recordstore

A versioned key→record store over a content-addressed chunk store — a thin
database kernel between an immutable blob store (such as [Ethereum
Swarm](https://www.ethswarm.org/)) and an application that wants to think in
records and versions rather than chunks and references.

```python
from recordstore import RecordStore, MemoryChunkStore

chunks = MemoryChunkStore()
store = RecordStore(chunks)
store.put("users/alice", {"name": "Alice", "role": "admin"})
store.put("users/bob", {"name": "Bob"})
root = store.commit()          # one reference identifies this entire version

store.get("users/alice")       # {'name': 'Alice', 'role': 'admin'}
list(store.keys("users/"))     # ['users/alice', 'users/bob']

snapshot = RecordStore.at(root, chunks)   # frozen view of that version
```

## Why

Content-addressed stores give you immutable `put(bytes) → ref` /
`get(ref) → bytes` and nothing else: no keys, no typed records, no
transactions, no snapshots. `recordstore` adds exactly that missing layer
and nothing more:

- **Records instead of raw bytes** — values are any JSON-compatible object,
  stored under string keys.
- **Atomic, versioned commits** — mutations are staged in memory;
  `commit()` lands all of them as one new **root reference**. A reader
  either sees all of a commit or none of it.
- **Snapshot isolation** — `RecordStore.at(root, chunks)` pins one root and
  sees a frozen, self-consistent dataset for arbitrarily long reads, with
  no locking: the whole dataset-at-a-version *is* one reference.
- **Canonical roots** — encodings are deterministic, so **equal content
  produces an equal root reference**, regardless of the insertion/deletion
  history that produced it. Versions are content-addressable, comparable
  with a string equality check, and cheap to diff and merge above this
  layer.
- **Structural sharing** — versions are stored as a persistent
  (copy-on-write) compacted radix trie; a commit writes only the chunks
  along the changed paths, and unchanged subtrees are shared between
  versions.

## Install

```bash
pip install "recordstore @ git+https://github.com/petfold/recordstore.git@v0.1.1"

# with the Bee (Swarm) backend's HTTP dependency:
pip install "recordstore[bee] @ git+https://github.com/petfold/recordstore.git@v0.1.1"
```

Python ≥ 3.9. The core imports only the standard library; `requests` is
needed (and imported lazily) only by `BeeBytesStore`.

## The pieces

| Layer | What it does | Implementations |
|---|---|---|
| `ChunkStore` | `put(bytes) → ref`, `get(ref) → bytes` | `MemoryChunkStore` (in-memory, testing), `BeeBytesStore` (Swarm Bee node over `/bytes` — the blob endpoint, not the raw `/chunks/{address}` primitive) |
| trie (internal) | canonical persistent radix trie mapping keys to value chunks | — |
| `RecordStore` | staging, `commit() → root`, snapshots, sorted prefix iteration | — |
| `Pointer` | mutable name for the latest root | `MemoryPointer`, `FilePointer` (atomic local file), `SwarmFeedPointer` (documented stub) |

Nothing above `RecordStore` ever sees a chunk or a trie node.

## Documentation

- **[User guide](docs/USER_GUIDE.md)** — concepts, full API, the canonicity
  contract, running against a real Bee node, versioning patterns, error
  handling, and current limitations.

## Testing

```bash
python3 -m pytest tests/                                 # unit + fuzz + boundary tests

BEE_API=http://<node>:1633 BEE_BATCH=<batchID> \
    python3 -m pytest tests/test_recordstore_bee.py -v   # against a live Bee node
```

The fuzz suite runs randomized put/delete histories against a plain-dict
oracle and asserts the canonical-root property throughout. The Bee
integration tests skip automatically unless `BEE_API` is set; against a
real (non-dev) node always provide `BEE_BATCH` with a purchased postage
batch id.

## Background

This is a Python re-implementation of an old idea — content-addressed,
canonical-root, versioned key-value storage — best known from Ethereum's
Merkle Patricia Trie and from Noms/Dolt's "prolly trees." The value here is
fit, not novelty: a much simpler canonical encoding than MPT (avoiding the
exact bug class that once caused a chain split), a far smaller scope than
Dolt/Irmin (no query language, no built-in merge), and — as far as we could
find — the first implementation of this pattern for Python with a Swarm/Bee
backend. See the [user guide's background section](docs/USER_GUIDE.md#0-background-is-this-reinventing-the-wheel)
for the full comparison.

## Status

Extracted from [petfold/ontodag](https://github.com/petfold/ontodag)
(July 2026) with history preserved; validated against a live Bee 2.8.1
light node on Gnosis mainnet (roundtrips, canonical roots on real BMT
references, network retrievability). Known gaps — single-writer only (no
concurrency control), `SwarmFeedPointer` not yet implemented, one chunk
per record — are detailed in the
[user guide](docs/USER_GUIDE.md#limitations-and-roadmap).
