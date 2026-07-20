# recordstore — User Guide

`recordstore` is a versioned key→record store layered over any
content-addressed bytes store. This guide covers the concepts, the full
public API, the canonicity contract, running against a real Swarm/Bee node,
common versioning patterns, and current limitations.

Everything documented here is importable from the top-level package:

```python
from recordstore import (
    RecordStore,
    MemoryBytesStore, BeeBytesStore,
    MemoryPointer, FilePointer, SwarmFeedPointer,
    canonical_bytes,
)
```

---

## 0. Background: is this reinventing the wheel?

Short answer: the core idea isn't new, but this particular instantiation
fills a real gap. Worth reading before you decide whether to build on this
vs. something else.

### The idea has prior art, and a well-documented failure mode

"Content-addressed trie → canonical root, independent of edit history" is
the same idea behind Ethereum's **Merkle Patricia Trie**, and behind
**Noms** (Attic Labs, Go, now dead/archived) and its living successor
**Dolt** ("Git for data"), whose "prolly trees" achieve the same property
via content-defined chunking rather than pure key-branching. Academically,
Auvolat & Taïani's **Merkle Search Trees** (2019) formalize exactly this
requirement and prove ordinary B-trees don't have it — insertion order can
change their shape even with identical final content — tracing back to
Naor & Teague's 2001 work on history-independent data structures.

Getting the canonical-form invariants wrong has real consequences, not just
theoretical ones:

- Ethereum's MPT needs hex-prefix encoding to disambiguate leaf vs.
  extension nodes and odd/even nibble parity — precisely the "empty node
  with one child" ambiguity this trie's canonicalization rules close.
  Early implementations (`geth` vs `ethereumj`) diverged on root hash after
  just two inserts into an empty trie.
- In November 2016, `geth` and Parity handled an edge case (empty-account
  deletion under EIP-161) differently under out-of-gas conditions,
  producing different state roots for identical transaction histories —
  an actual chain fork at block 2,686,351.
- IPFS's own UnixFS HAMT is **not** canonical — its CID depends on chunk
  size and DAG balancing, a limitation still being addressed years later
  (IPIP-499).

So: people keep needing this, keep reinventing it, and keep getting subtle
parts of it wrong. This library's two canonicalization rules (§1, "The
canonicity guarantee") are the minimal fix for the same failure class MPT
took years of bugs to close — closer in spirit to MPT's pure radix-branching
approach than to prolly trees' content-defined chunking, which trades a
probabilistic depth-balancing guarantee (useful under adversarial or
long-shared-prefix key sets) for a mechanism this codebase doesn't need at
its current scale.

Related but distinct ideas, for context: **Merkle-CRDTs** (Protocol Labs)
and Git's own object model don't have this property, for different
reasons — Merkle-CRDTs are a causal DAG of deltas with no single
"current state" root, and Git trees, while genuinely canonical, are
hierarchical paths rather than a flat key-value map with point lookup.

### Where the actual value is

Not the idea — the fit:

- **Simpler encoding than MPT, in exactly the place MPT kept breaking.**
  Plain byte prefixes and canonical JSON instead of nibble-packed RLP:
  same canonicity guarantee, far less surface area for the encoding bugs
  above.
- **Much smaller than its closest relatives.** Dolt is a full SQL database;
  Irmin is a general Git-like store with branching and merge built in.
  This library deliberately has neither — merge is left to the application
  layer (see `docs/SWARM_DESIGN.md` §5 in the OntoDAG repo) — because the
  consuming use case only ever needed put/get/commit/snapshot with a
  canonical root, not a database engine.
- **First Python + Swarm instance of this pattern, as far as we found.**
  Noms is dead, Dolt and Irmin are Go/OCaml with much larger footprints,
  and IPLD's own "prolly tree" ADL spec isn't finalized. Nothing turned up
  filling "small, dependency-light, Python, pluggable content-addressed
  backend including Bee."

### If you need cross-language interop

If roots ever need to be produced identically from Go, JS, or another
language, the relevant prior art is **IPLD**'s approach: a CID tags both
codec and hash function, and **dag-cbor**/**dag-json** are *deterministic*
codec specs (sorted map keys, no duplicate keys, shortest-form numbers) —
not "canonical JSON" left to each language's own encoder. IPLD backs this
with published cross-language test fixtures (hex blocks + expected CIDs)
that Go/JS/Python implementations must all reproduce. That fixture-suite
approach — not just a prose spec — is what would have caught the
`geth`/`ethereumj` divergence before it shipped, and is the template to
follow if this store's wire format ever needs a second-language
implementation.

---

## 1. Concepts

### Records

A **record** is any JSON-compatible value — `dict`, `list`, `str`, number,
bool, `None` — stored under a **non-empty string key**. Keys are plain
strings with no imposed structure; because iteration is sorted and
prefix-filtered, `/`-separated prefixes (`"users/alice"`) give you cheap
namespacing for free.

### Roots and versions

All records live in a persistent (copy-on-write) compacted radix trie whose
nodes are stored as blobs in the bytes store. The trie's **root reference** — a hex
string — identifies one immutable, self-consistent snapshot of the *entire*
dataset. A root is to a dataset what a commit hash is to a git tree:

- hold a root ⇒ you can read that exact version forever (as long as the
  blobs exist),
- compare two roots with `==` ⇒ you know whether two datasets are identical,
- share a root ⇒ someone else can open the same version.

### Staging and commit

A `RecordStore` accumulates `put`/`delete` calls **in memory**. Nothing
touches the bytes store until `commit()`, which writes the changed records
and trie paths and returns the new root. Reads are
**read-your-writes**: staged changes shadow the committed state, so
`get`/`keys`/`contains` always reflect what you would see after committing.

Because versions share structure, a commit writes only the blobs along the
paths that changed; everything else is reused from the previous version.

### The canonicity guarantee

> **Equal content ⇒ equal root.** Two datasets containing the same
> key→value pairs have byte-identical roots, no matter what sequence of
> puts, deletes, and commits produced them.

This holds because every encoding in the stack is deterministic:

- **Values** are encoded with `canonical_bytes` — JSON with sorted object
  keys, minimal separators, UTF-8, `NaN`/`Infinity` rejected (they have no
  canonical JSON form). Two structurally equal values always produce the
  same bytes, hence the same blob reference.
- **Trie nodes** are canonically encoded, and the trie maintains two
  structural invariants so its *shape* is a pure function of its contents:
  a node with no value and no children does not exist, and a node with no
  value and exactly one child is merged into that child.

Consequences you can rely on:

- deduplication — identical values are stored once;
- O(1) version comparison — `root_a == root_b`;
- history independence — replaying operations in any order that reaches the
  same final content reaches the same root (this is the substrate for
  building CRDT-style merges above this layer);
- idempotent commits — committing with nothing staged (or with staged
  writes equal to what's already stored) returns the same root.

One subtlety: JSON does not distinguish `1` from `1.0`, and Python types
that JSON normalizes (tuples become lists) come back in their JSON form.
Store what JSON can represent faithfully.

`canonical_bytes(value)` is exported for anything that needs byte-identical
encodings of its own (hashing application-level objects, testing).

---

## 2. `RecordStore` API

### Constructing

```python
RecordStore(bytes_store, root=None, pointer=None)
```

- `bytes_store` — any object satisfying the `BytesStore` protocol (§3).
- `root` — open at an existing version. `None` starts from the empty
  dataset (or from the pointer's value, see next).
- `pointer` — any object satisfying the `Pointer` protocol (§4). If given
  and `root` is `None`, the store opens at `pointer.get()`; every
  successful `commit()` then advances the pointer to the new root.

```python
RecordStore.at(root, bytes_store)   # classmethod
```

Opens a **read-only snapshot** at `root`. Reads work as usual;
`put`/`delete`/`commit` raise `TypeError`. Use this whenever you want to
read a version without any risk of writing.

```python
store.root   # property
```

The root of the last committed state (staged changes are *not* included —
it changes only on `commit()`). `None` for a brand-new empty store.

### Reading

```python
store.get(key)         # → value (deep copy); raises KeyError if absent
store.contains(key)    # → bool
store.keys(prefix="")  # → iterator of keys, sorted, staged overlay included
store.items(prefix="") # → iterator of (key, value), sorted, staged included
```

- Returned values are **deep copies** — mutating them never mutates the
  store. Write changes back with `put`.
- `keys("users/")` iterates only keys starting with that prefix, in sorted
  order. The prefix is a plain string prefix, not a path component — 
  `keys("users")` also matches `"users2/x"`.
- `items()` is the way to read many records at once: it fetches the value
  blobs in a single batch, so over `BeeBytesStore` the reads run concurrently
  instead of one serial round trip per record. Prefer it over `keys()` +
  `get()` in a loop when hydrating a whole store or prefix (see §6).

### Writing

```python
store.put(key, value)  # stage an insert/overwrite
store.delete(key)      # stage a removal; raises KeyError if absent
```

- `put` validates immediately: the key must be a non-empty string
  (`ValueError` otherwise) and the value must be canonically encodable
  (`TypeError`/`ValueError` from the JSON encoder otherwise — this fails
  fast at `put` time, never at `commit` time).
- `put` stores a detached copy of the value: mutating your object after
  `put` does not change what was staged.
- Staged changes are read-your-writes and can overwrite each other freely;
  only the final staged state matters at commit.

### Committing

```python
root = store.commit()  # → new root reference (hex string), or None if the
                       #   dataset is empty
```

Flushes staged changes in deterministic (sorted-key) order, updates
`store.root`, advances the pointer if one is attached, and returns the new
root. The pointer moves only after every blob write has succeeded, so a
reader following the pointer sees all of a commit or none of it.

Deleting a key that was staged-but-never-committed simply drops it;
committing a delete of a key that never existed in the trie is a no-op.

### Error summary

| Situation | Raised |
|---|---|
| `get`/`delete` of a missing key | `KeyError` |
| empty or non-string key | `ValueError` |
| non-JSON-encodable value in `put` | `TypeError` / `ValueError` |
| `NaN` / `Infinity` in a value | `ValueError` |
| write on a read-only snapshot | `TypeError` |
| blob missing from the bytes store | `KeyError` (from the backend) |

---

## 3. Bytes store backends

The `BytesStore` protocol is two methods:

```python
class BytesStore(Protocol):
    def put(self, data: bytes) -> str: ...   # → reference
    def get(self, ref: str) -> bytes: ...    # KeyError if missing
```

Any content-addressed store satisfying it works — the reference just has to
be a stable hex string determined by the content.

### `MemoryBytesStore()`

A dict keyed by SHA-256. Use it for tests and ephemeral work; `len(store)`
gives the blob count. Data lives only as long as the object.

### `BeeBytesStore(api_url, postage_batch_id, deferred_upload=True)`

A real [Swarm Bee](https://docs.ethswarm.org/) node over its HTTP API
(`POST`/`GET /bytes`) — named for that endpoint specifically: `/bytes` is
Bee's blob-level API, distinct from the raw `/chunks/{address}` single-chunk
primitive that this class does not use. References are Swarm BMT
references. Requirements and behavior:

- **`requests`** is imported lazily inside the constructor — install the
  `[bee]` extra.
- **A usable postage batch id is required for writes.** Against a real
  (mainnet) node, always purchase a batch yourself and pass its id;
  batches below ~1 day of validity are rejected by the network, and a
  fresh purchase takes on the order of a minute to become usable.
- `deferred_upload=True` (default) returns as soon as the node has the
  data locally, with push-sync to the network happening in the background;
  `False` waits. Check network retrievability with Bee's
  `GET /stewardship/{ref}` if you need the guarantee.
- Values larger than one 4 KB chunk are handled transparently by Bee's
  splitter — any payload yields exactly one reference.
- HTTP timeouts are 120 s; a 404 surfaces as `KeyError`, other HTTP errors
  as `requests.HTTPError`.

A quick smoke against a local node:

```python
from recordstore import RecordStore, BeeBytesStore

blobs = BeeBytesStore("http://localhost:1633", "<batch-id>")
store = RecordStore(blobs)
store.put("hello", {"world": True})
root = store.commit()
print(RecordStore.at(root, blobs).get("hello"))
```

Because chunks are immutable and content-addressed, mixing backends is
safe in one direction: anything written through one `BeeBytesStore` is
readable through any other Bee node that can retrieve the chunks.

---

## 4. Pointers

A root reference identifies a version forever, but something has to name
“the latest version.” That is a `Pointer`:

```python
class Pointer(Protocol):
    def get(self) -> Optional[str]: ...
    def set(self, root: str) -> None: ...
```

Attach one at construction and it is read at open and advanced on every
commit:

```python
from recordstore import RecordStore, MemoryBytesStore, FilePointer

pointer = FilePointer("/var/lib/myapp/ROOT")
store = RecordStore(blobs, pointer=pointer)   # opens at the pointed root
store.put("k", 1)
store.commit()                                  # pointer now names the new root
```

- **`MemoryPointer(root=None)`** — in-process only.
- **`FilePointer(path)`** — one root in a local file; `set` writes a temp
  file and `os.replace`s it, which is atomic on POSIX, so a crash never
  leaves a torn pointer. A missing file reads as `None`.
- **`SwarmFeedPointer(api_url, topic, *, signer=None, owner=None,
  postage_batch_id=None, ...)`** — the "latest root" as an owner-signed
  Swarm feed. Each `set` publishes a signed single-owner chunk (SOC);
  `get` resolves the latest via a feed lookup. Needs the `swarm-bee`
  package (`pip install "recordstore[feeds]"`) for the BMT/secp256k1
  signing, imported lazily so the core stays stdlib-only.

  Pass a `signer` (32-byte secp256k1 private key, hex) to read and write —
  the owner address is derived from it; or an `owner` address (hex) for a
  read-only pointer. Writing also needs a `postage_batch_id`. `topic` is a
  namespace string hashed to the feed topic.

  Because Swarm feed *lookups* are unreliable per call on a light node
  (transient 404s, or a stale-early index — see §7), `SwarmFeedPointer`
  does not trust a single lookup: it serves your own writes from a
  read-your-writes cache (`feed_ttl` seconds), floors the write index
  monotonically so back-to-back commits never collide, and retries cold
  reads with exponential backoff, ignoring a result that regresses below
  the newest index it has seen. A never-written feed resolves to `None`
  after the retries exhaust (which `RecordStore` treats as the empty
  dataset). The retry/backoff/TTL knobs are constructor arguments.

---

## 5. Versioning patterns

**Time travel.** Keep the roots your application cares about (commit ids,
checkpoints); open any of them later:

```python
v1 = store.commit()
store.put("config", {"mode": "fast"})
v2 = store.commit()

old = RecordStore.at(v1, blobs)   # v1 is untouched by later commits
```

**Long consistent reads.** Snapshots are immutable, so a reporting job can
iterate `keys()` and `get()` for hours against one root while writers
commit new versions concurrently — no locks, no torn reads.

**Cheap change detection.** Two stores (or two moments of one store) are
equal iff their roots are equal. To sync, compare roots first and walk keys
only on mismatch.

**Branching and merge.** Open two stores at the same root, let them diverge,
and commit each — two versions sharing most of their structure (equal-content
branches converge to the same root by canonicity). Reconcile them with a
three-way merge against their common ancestor:

```python
from recordstore import RecordStore, MergeConflict, ABSENT, DELETE

merged = RecordStore.merge(blobs, base_root, our_root, their_root)
```

A change made on only one side is taken; the same change on both sides is
taken once; a change made on both sides to *different* values is a conflict.
By default a conflict raises `MergeConflict` (its `.conflicts` lists the
keys) — nothing is silently dropped. Supply a `resolver` to settle them:

```python
def resolver(key, base, ours, theirs):   # each is the value or ABSENT
    if ours is ABSENT or theirs is ABSENT:
        return DELETE                    # e.g. delete wins over modify
    return max(ours, theirs)             # your policy

merged = RecordStore.merge(blobs, base, ours, theirs, resolver=resolver)
```

The merge is efficient by canonicity: both the read (a structural diff that
prunes subtrees equal on both sides) and the write (only the merged diff,
applied to `base`) are proportional to the divergence, not the dataset. It is
commutative when the resolver is symmetric in its ours/theirs arguments (the
built-in raise-on-conflict is).

**Automatic reconciliation.** With a pointer attached you rarely call `merge`
by hand — pass `reconcile=True` to `commit`, and it converges with concurrent
writers instead of overwriting them:

```python
store = RecordStore(blobs, pointer=pointer)   # opens at the pointer's root
store.put("users/alice", {...})
store.commit(reconcile=True, resolver=resolver)   # merges if the pointer moved
```

If the pointer still points where this store branched from, the commit lands
directly; if another writer advanced it, `commit` three-way merges the two and
retries (up to `retries=`) until it lands. A pointer exposing
`compare_and_set` (e.g. `MemoryPointer`) gets race-free updates; `FilePointer`
/ `SwarmFeedPointer` fall back to a best-effort read-then-set.

**Idempotent writers.** A writer that recomputes and re-puts the same
records produces the same root — safe to re-run, and downstream consumers
comparing roots see “no change.”

---

## 6. Performance notes

- **One record = one value blob**, plus one blob per trie node along the
  key's path. Trie nodes are compacted (radix), so path length tracks key
  *distinctiveness*, not key length.
- **Commits write O(changed paths) blobs**, not O(dataset).
- **Trie nodes are cached in memory** after first load (they are immutable,
  so the cache never invalidates). Re-reading the same region of a store or
  snapshot is cheap; a cold open pays one blob fetch per trie level per
  distinct path.
- Reads through `BeeBytesStore` are one HTTP roundtrip per (uncached) blob.
  To hydrate an entire dataset (or a prefix) use `items()` rather than
  `keys()` + `get()` per key: it batches the value-blob reads (and the trie
  walk fetches each node's children as a batch too), so `BeeBytesStore` fetches
  them concurrently instead of one serial round trip at a time — a large win on
  a high-latency link. Tune the parallelism with `BeeBytesStore(...,
  max_concurrent_reads=N)` (default 16).
- `BeeBytesStore` keeps a pooled, keep-alive HTTP session, so no blob op pays a
  fresh TCP/TLS handshake — the single biggest per-op saving on a slow link.
- On write, `commit()` writes bottom-up in concurrent batches: all value blobs
  first, then the trie nodes one level at a time (children before parents,
  since a parent's reference is the Bee-assigned hash of its children). Only the
  nodes surviving in the final root are written — orphaned intermediates from
  key-by-key insertion are pruned. A commit therefore costs roughly *O(trie
  depth) concurrent round-trip rounds*, not one serial round trip per node.
  Still prefer fewer, larger commits on a high-latency link (fewer feed-pointer
  updates and fewer depth rounds overall).

---

## 7. Limitations and roadmap

- **Concurrency control needs an opt-in and a CAS-capable pointer.**
  `commit(reconcile=True)` (§5) makes concurrent writers converge — it
  three-way merges and retries when the pointer moved under it — and the plain
  `commit()` still last-write-wins. Reconciliation is race-free only against a
  pointer that implements `compare_and_set` (`MemoryPointer` does, so in-process
  multi-writer is correct); `FilePointer` and `SwarmFeedPointer` fall back to a
  best-effort read-then-set, which narrows but does not close the window between
  two writers landing. A CAS for the feed pointer (write-at-expected-index) is
  the remaining step for fully safe cross-process multi-writer.
- **Swarm feed lookups are unreliable per call.** `SwarmFeedPointer`
  (implemented in v0.4.0, see §4) works around this — read-your-writes cache,
  monotonic write-index floor, and retry-until-stable reads with a stale-early
  guard, following swarmfs's `bzzf://` layer. As of v0.4.1 it also passes Bee's
  `after` index hint once it has a confirmed index to resume from, so lookups
  start near the tip; because `swarm-bee`'s typed API does not expose `after`
  (see bee-py#2) this goes through the client transport. As of v0.4.1 index
  discovery no longer depends on the flaky lookup at all — it probes the feed's
  SOC chunks directly (individually retrievable even when the lookup 404s), so
  cold reads resolve in one attempt and an empty feed returns `None` at once.
  The one remaining rough edge is that the `after` hint reaches Bee through a
  private `swarm-bee` transport surface until bee-py#2 exposes it publicly.
  Full rationale is in the `SwarmFeedPointer` docstring in `recordstore.py`.
- **Concurrency tuning across a real link.** The read/write parallelism cap
  (`BeeBytesStore(max_concurrent_reads=…)`, default 16) is a single per-store
  value; its optimum depends on the client↔node link and is best found with a
  two-node benchmark — writer and reader on separate nodes (ideally separate
  locations) so reads force real Swarm retrieval rather than local-store hits.
  That measurement may also motivate splitting the cap into separate read/write
  limits.
- **No garbage collection.** Old versions' blobs are never deleted by this
  library. On Swarm, chunk lifetime is governed by postage stamps and the
  network's GC — content simply expires unless re-stamped or pinned; for
  `MemoryBytesStore` everything lives until the process exits.
- **Record schema version.** Every value blob is wrapped in
  `{"rsv": 1, "val": ...}`; a future format bump will change `rsv` and
  readers reject unknown versions rather than misread them. Trie nodes
  carry an analogous `"tn": 1`.

---

## 8. Testing your own usage

The test suite doubles as executable documentation:

- `tests/test_recordstore.py` — the API contract: canonical roots, snapshot
  isolation, structural sharing, no aliasing, pointer atomicity.
- `tests/test_recordstore_fuzz.py` — randomized put/delete histories against
  a dict oracle, asserting the canonical-root property throughout.
- `tests/test_recordstore_bee.py` — the same store over a live Bee node
  (`BEE_API`/`BEE_BATCH` env vars; skips otherwise).
- `tests/test_recordstore_feed.py` — `SwarmFeedPointer` over a live Bee node
  (read-your-writes, network resolution, read-only pointer, end-to-end
  `RecordStore` reopen); skips unless `BEE_API` is set and `swarm-bee` is
  installed.
- `tests/test_boundaries.py` — enforces that module-level imports stay
  stdlib-only.

When building on the `BytesStore` or `Pointer` protocols, the
`MemoryBytesStore`/`MemoryPointer` pairing plus the fuzz test's
oracle pattern is a good template for validating an implementation.
