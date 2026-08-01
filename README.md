# recordstore

[![tests](https://github.com/petfold/recordstore/actions/workflows/tests.yml/badge.svg)](https://github.com/petfold/recordstore/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/recordstore)](https://pypi.org/project/recordstore/)
[![license](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

A versioned key→record store over a content-addressed bytes store — a thin
database kernel between an immutable blob store (such as [Ethereum
Swarm](https://www.ethswarm.org/)) and an application that wants to think in
records and versions rather than blobs and references.

```python
from recordstore import RecordStore, MemoryBytesStore

blobs = MemoryBytesStore()
store = RecordStore(blobs)
store.put("users/alice", {"name": "Alice", "role": "admin"})
store.put("users/bob", {"name": "Bob"})
root = store.commit()          # one reference identifies this entire version

store.get("users/alice")       # {'name': 'Alice', 'role': 'admin'}
list(store.keys("users/"))     # ['users/alice', 'users/bob']

snapshot = RecordStore.at(root, blobs)   # frozen view of that version
```

Concurrent writers converge without a lock server — if the shared pointer
moved under a commit, it three-way merges and retries:

```python
from recordstore import MemoryPointer

pointer = MemoryPointer(root)                  # a shared "latest version" name
a = RecordStore(blobs, pointer=pointer)        # two writers open the same version
b = RecordStore(blobs, pointer=pointer)
a.put("users/carol", {"name": "Carol"}); a.commit(reconcile=True)
b.put("users/dave",  {"name": "Dave"});  b.commit(reconcile=True)   # folds in a's change
# the pointer now names a version containing both carol and dave
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
- **Snapshot isolation** — `RecordStore.at(root, blobs)` pins one root and
  sees a frozen, self-consistent dataset for arbitrarily long reads, with
  no locking: the whole dataset-at-a-version *is* one reference.
- **Canonical roots** — encodings are deterministic, so **equal content
  produces an equal root reference**, regardless of the insertion/deletion
  history that produced it. Versions are content-addressable, comparable
  with a string equality check, and cheap to diff.
- **Three-way merge** — `RecordStore.merge(base, ours, theirs)` reconciles
  two divergent versions; canonicity makes unchanged subtrees merge for free
  and equal edits conflict-free, with conflicts raised or settled by a
  `resolver(key, base, ours, theirs)` you supply. The diff is O(divergence),
  not O(dataset).
- **Verifiable proofs** — `store.prove(key)` produces a small, JSON-ready
  inclusion **or absence** proof against the committed root (absence is
  provable because the canonical encoding gives a key exactly one possible
  location); `verify_proof(proof, root)` checks it with no store access at
  all — hold the 32-byte root, verify any claim about the dataset.
- **Version comparison** — `store.diff(other_root)` answers "what changed
  between these two published versions?" directly: `(key, mine, theirs)` per
  differing key, the same O(divergence) structural walk, so equal roots read
  nothing at all.
- **Multi-writer, no lock server** — `commit(reconcile=True)` makes concurrent
  writers converge: if the pointer moved under you it three-way merges and
  retries instead of overwriting. Race-free in-process; best-effort across
  processes over a Swarm feed. A commutative resolver keeps 3+ writers
  order-independent.
- **Structural sharing** — versions are stored as a persistent
  (copy-on-write) compacted radix trie; a commit writes only the blobs
  along the changed paths, and unchanged subtrees are shared between
  versions.
- **Concurrent I/O** — `items()` (bulk read), `get_many`/`put_many`, and a
  pooled keep-alive HTTP session let `BeeBytesStore` parallelise round trips
  instead of paying one per record — the difference between usable and not on
  a high-latency link.

## Install

```bash
pip install recordstore

# with the Bee (Swarm) bytes backend's HTTP dependency:
pip install "recordstore[bee]"

# with the Swarm feed pointer (adds swarm-bee for SOC/secp256k1 signing):
pip install "recordstore[feeds]"

# with postage_batch_id="auto" and batch-health reporting (adds swarmfs):
pip install "recordstore[stamps]"
```

Python ≥ 3.11. The core imports only the standard library; both extra
dependencies are imported lazily — `requests` only by `BeeBytesStore`
(`[bee]`), `swarm-bee` only by `SwarmFeedPointer` (`[feeds]`).

## The pieces

| Layer | What it does | Implementations |
|---|---|---|
| `BytesStore` | `put(bytes) → ref`, `get(ref) → bytes` | `MemoryBytesStore` (in-memory, testing), `DirBytesStore` (durable local directory), `FsspecBytesStore` (S3/GCS/HTTP/… via fsspec), `BeeBytesStore` (Swarm Bee node over `/bytes` — the blob endpoint, not the raw `/chunks/{address}` primitive) |
| trie (internal) | canonical persistent radix trie mapping keys to value blobs | — |
| `RecordStore` | staging, `commit()` / `commit(reconcile=True)`, snapshots, sorted `keys()`/`items()`, three-way `merge()`, structural `diff()` | — |
| `Pointer` | mutable name for the latest root | `MemoryPointer`, `FilePointer` (atomic local file), `SwarmFeedPointer` (owner-signed Swarm feed, over `swarm-bee`) |

| `swarm_store(topic, ...)` | assembles the two Swarm pieces into a store | the one place Swarm is chosen: `BeeBytesStore` blobs **and** a `SwarmFeedPointer` head |

Nothing above `RecordStore` ever sees a stored blob or a trie node — and
nothing below it needs to know Swarm exists unless you asked for it:

```python
from recordstore import swarm_store

store = swarm_store("my-notes", signer=key)   # publish: blobs + feed on Swarm
store = swarm_store("my-notes", owner=addr)   # follow someone else's feed
```

## Documentation

- **[User guide](docs/USER_GUIDE.md)** — concepts, full API, the canonicity
  contract, running against a real Bee node, versioning patterns, error
  handling, and current limitations.

## Testing

```bash
python3 -m pytest tests/                                 # unit + fuzz + boundary tests

BEE_API=http://<node>:1633 BEE_BATCH=<batchID> \
    python3 -m pytest tests/test_recordstore_bee.py -v   # bytes backend, live node

pip install "recordstore[feeds]"                         # needs swarm-bee
BEE_API=http://<node>:1633 BEE_BATCH=<batchID> \
    python3 -m pytest tests/test_recordstore_feed.py -v  # feed pointer, live node
```

The fuzz suite runs randomized put/delete histories against a plain-dict
oracle and asserts the canonical-root property throughout. The Bee
integration tests skip automatically unless `BEE_API` is set (the feed test
also needs `swarm-bee` installed); against a real (non-dev) node always
provide `BEE_BATCH` with a purchased postage batch id.

## Background

This is a Python re-implementation of an old idea — content-addressed,
canonical-root, versioned key-value storage — best known from Ethereum's
Merkle Patricia Trie and from Noms/Dolt's "prolly trees." The value here is
fit, not novelty: a much simpler canonical encoding than MPT (avoiding the
exact bug class that once caused a chain split), a far smaller scope than
Dolt/Irmin (no query language; a single three-way merge primitive, not a
merge engine), and — as far as we could
find — the first implementation of this pattern for Python with a Swarm/Bee
backend. See the [user guide's background section](docs/USER_GUIDE.md#0-background-is-this-reinventing-the-wheel)
for the full comparison.

## Status

Extracted from [petfold/ontodag](https://github.com/petfold/ontodag)
(July 2026) with history preserved; validated against a live Bee 2.8.1
light node on Gnosis mainnet (roundtrips, canonical roots on real BMT
references, network retrievability). `SwarmFeedPointer` (owner-signed Swarm
feed, over `swarm-bee`) landed in v0.4.0; three-way `merge` in v0.8.0;
auto-reconciling `commit(reconcile=True)` in v0.9.0; a best-effort feed
`compare_and_set` (cross-process reconcile) in v0.10.0. Known gaps — a Swarm
feed has no atomic index claim, so exactly-simultaneous same-index writes can
still race (in-process reconcile is race-free); one blob per record — are
detailed in the [user guide](docs/USER_GUIDE.md#limitations-and-roadmap).
